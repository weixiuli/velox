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

#pragma once

#include "velox/common/memory/RawVector.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

/// Helper class used to build a hash join table outside of the operator
/// pipeline. This is primarily intended for broadcast hash join scenarios where
/// the build side is constructed on one node and transferred to others. The API
/// mirrors the relevant parts of HashBuild::addInput / finish logic.
class HashTableBuilder {
 public:
  HashTableBuilder(
      memory::MemoryPool* pool,
      RowTypePtr inputType,
      std::vector<int32_t> keyChannels,
      bool ignoreNullKeys,
      bool allowDuplicates,
      bool hasProbedFlag,
      uint32_t minTableSizeForParallelJoinBuild);

  /// Adds a batch of build-side rows to the hash table.
  void addInput(const RowVectorPtr& input);

  /// Finalizes and returns the built hash table.
  std::shared_ptr<BaseHashTable> build();

  RowTypePtr tableType() const {
    return tableType_;
  }

 private:
  void setupDecoders(const RowTypePtr& inputType);
  void setupTable(memory::MemoryPool* pool);

  memory::MemoryPool* pool_;
  RowTypePtr inputType_;
  RowTypePtr tableType_;
  const std::vector<int32_t> keyChannels_;
  std::vector<int32_t> dependentChannels_;
  std::vector<std::unique_ptr<DecodedVector>> decoders_;
  SelectivityVector activeRows_;
  raw_vector<uint64_t> hashes_;
  bool analyzeKeys_{false};
  const bool ignoreNullKeys_;
  const bool allowDuplicates_;
  const bool hasProbedFlag_;
  const uint32_t minTableSizeForParallelJoinBuild_;
  bool built_{false};
  std::unique_ptr<BaseHashTable> table_;
};

} // namespace facebook::velox::exec
