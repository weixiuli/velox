/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/exec/HashTableBuilder.h"

#include <algorithm>
#include <numeric>

#include <fmt/format.h>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

HashTableBuilder::HashTableBuilder(
    memory::MemoryPool* pool,
    RowTypePtr inputType,
    std::vector<int32_t> keyChannels,
    bool ignoreNullKeys,
    bool allowDuplicates,
    bool hasProbedFlag,
    uint32_t minTableSizeForParallelJoinBuild)
    : pool_(pool),
      inputType_(std::move(inputType)),
      keyChannels_(std::move(keyChannels)),
      ignoreNullKeys_(ignoreNullKeys),
      allowDuplicates_(allowDuplicates),
      hasProbedFlag_(hasProbedFlag),
      minTableSizeForParallelJoinBuild_(minTableSizeForParallelJoinBuild),
      hashes_(pool_) {
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_NOT_NULL(inputType_);
  VELOX_CHECK(!keyChannels_.empty(), "Hash table requires at least one key");

  setupDecoders(inputType_);
  setupTable(pool_);
  analyzeKeys_ = table_->hashMode() != BaseHashTable::HashMode::kHash;
}

void HashTableBuilder::setupDecoders(const RowTypePtr& inputType) {
  std::vector<bool> isKey(inputType->size(), false);
  for (auto channel : keyChannels_) {
    VELOX_CHECK_GE(channel, 0);
    VELOX_CHECK_LT(
        channel,
        inputType->size(),
        "Key channel {} is out of range for input size {}",
        channel,
        inputType->size());
    isKey[channel] = true;
  }

  dependentChannels_.reserve(inputType->size() - keyChannels_.size());
  for (vector_size_t i = 0; i < inputType->size(); ++i) {
    if (!isKey[i]) {
      dependentChannels_.push_back(i);
      decoders_.emplace_back(std::make_unique<DecodedVector>());
    }
  }

  std::vector<std::string> names;
  names.reserve(inputType->size());
  for (vector_size_t i = 0; i < inputType->size(); ++i) {
    names.push_back(fmt::format("c{}", i));
  }
  std::vector<TypePtr> childTypes;
  childTypes.reserve(inputType_->size());
  for (vector_size_t i = 0; i < inputType_->size(); ++i) {
    childTypes.push_back(inputType_->childAt(i));
  }
  tableType_ = ROW(std::move(names), std::move(childTypes));
}

void HashTableBuilder::setupTable(memory::MemoryPool* pool) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.reserve(keyChannels_.size());
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    hashers.emplace_back(VectorHasher::create(
        inputType_->childAt(keyChannels_[i]), keyChannels_[i]));
  }

  std::vector<TypePtr> dependentTypes;
  dependentTypes.reserve(dependentChannels_.size());
  for (auto channel : dependentChannels_) {
    dependentTypes.push_back(inputType_->childAt(channel));
  }

  if (ignoreNullKeys_) {
    table_ = HashTable<true>::createForJoin(
        std::move(hashers),
        dependentTypes,
        allowDuplicates_,
        hasProbedFlag_,
        minTableSizeForParallelJoinBuild_,
        pool);
  } else {
    table_ = HashTable<false>::createForJoin(
        std::move(hashers),
        dependentTypes,
        allowDuplicates_,
        hasProbedFlag_,
        minTableSizeForParallelJoinBuild_,
        pool);
  }
}

void HashTableBuilder::addInput(const RowVectorPtr& input) {
  VELOX_CHECK(!built_, "Cannot add input after build() has been called");
  VELOX_CHECK_NOT_NULL(input);
  if (input->size() == 0) {
    return;
  }

  activeRows_.resize(input->size());
  activeRows_.setAll();

  auto& hashers = table_->hashers();
  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  if (ignoreNullKeys_) {
    deselectRowsWithNulls(hashers, activeRows_);
  }

  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(
        *input->childAt(dependentChannels_[i])->loadedVector(), activeRows_);
  }

  if (!activeRows_.hasSelections()) {
    return;
  }

  if (analyzeKeys_ && hashes_.size() < activeRows_.end()) {
    hashes_.resize(activeRows_.end());
  }

  if (analyzeKeys_) {
    for (auto& hasher : hashers) {
      hasher->computeValueIds(activeRows_, hashes_);
      analyzeKeys_ = analyzeKeys_ && hasher->mayUseValueIds();
    }
  }

  auto* rows = table_->rows();
  const auto nextOffset = rows->nextOffset();

  activeRows_.applyToSelected([&](auto rowIndex) {
    char* newRow = rows->newRow();
    if (nextOffset) {
      *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
    }
    for (auto i = 0; i < hashers.size(); ++i) {
      rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
    }
    for (auto i = 0; i < dependentChannels_.size(); ++i) {
      rows->store(
          *decoders_[i], rowIndex, newRow, i + hashers.size());
    }
  });
}

std::shared_ptr<BaseHashTable> HashTableBuilder::build() {
  VELOX_CHECK(!built_, "build() can only be called once");
  built_ = true;
  table_->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, nullptr);
  auto sharedTable = std::shared_ptr<BaseHashTable>(std::move(table_));
  return sharedTable;
}

} // namespace facebook::velox::exec
