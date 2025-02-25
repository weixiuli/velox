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

#include "velox/common/memory/Memory.h"

#include "velox/common/base/BitUtil.h"

DEFINE_bool(
    use_mmap_allocator_for_memory_pool,
    false,
    "If true, use MmapMemoryAllocator to allocate memory for MemoryPool");

namespace facebook {
namespace velox {
namespace memory {

/* static */
std::shared_ptr<MmapMemoryAllocator>
MmapMemoryAllocator::createDefaultAllocator() {
  return std::make_shared<MmapMemoryAllocator>();
}

void* FOLLY_NULLABLE MmapMemoryAllocator::alloc(int64_t size) {
  return mappedMemory_->allocateBytes(size);
}

void* FOLLY_NULLABLE
MmapMemoryAllocator::allocZeroFilled(int64_t numMembers, int64_t sizeEach) {
  auto totalBytes = numMembers * sizeEach;
  auto* allocResult = alloc(totalBytes);
  if (allocResult != nullptr) {
    std::memset(allocResult, 0, totalBytes);
  }
  return allocResult;
}

void* FOLLY_NULLABLE MmapMemoryAllocator::allocAligned(
    uint16_t /* alignment */,
    int64_t /* size */) {
  // TODO: Add functionality in MappedMemory to support allocAligned
  VELOX_UNSUPPORTED("allocAligned is not supported for MmapMemoryAllocator.");
}

void* FOLLY_NULLABLE MmapMemoryAllocator::realloc(
    void* FOLLY_NULLABLE p,
    int64_t size,
    int64_t newSize) {
  auto* newAlloc = alloc(newSize);
  if (p == nullptr || newAlloc == nullptr) {
    return newAlloc;
  }
  std::memcpy(newAlloc, p, std::min(size, newSize));
  free(p, size);
  return newAlloc;
}

void* FOLLY_NULLABLE MmapMemoryAllocator::reallocAligned(
    void* FOLLY_NULLABLE /* p */,
    uint16_t /* alignment */,
    int64_t /* size */,
    int64_t /* newSize */) {
  VELOX_UNSUPPORTED("reallocAligned is not supported for MmapMemoryAllocator.");
}

void MmapMemoryAllocator::free(void* FOLLY_NULLABLE p, int64_t size) {
  if (p == nullptr) {
    return;
  }
  mappedMemory_->freeBytes(p, size);
}

void* FOLLY_NULLABLE MemoryAllocator::alloc(int64_t size) {
  return std::malloc(size);
}

/* static */
std::shared_ptr<MemoryAllocator> MemoryAllocator::createDefaultAllocator() {
  return std::make_shared<MemoryAllocator>();
}

void* FOLLY_NULLABLE
MemoryAllocator::allocZeroFilled(int64_t numMembers, int64_t sizeEach) {
  return std::calloc(numMembers, sizeEach);
}

void* FOLLY_NULLABLE
MemoryAllocator::allocAligned(uint16_t alignment, int64_t size) {
  return aligned_alloc(alignment, size);
}

void* FOLLY_NULLABLE MemoryAllocator::realloc(
    void* FOLLY_NULLABLE p,
    int64_t /* size */,
    int64_t newSize) {
  return std::realloc(p, newSize);
}

void* FOLLY_NULLABLE MemoryAllocator::reallocAligned(
    void* FOLLY_NULLABLE p,
    uint16_t alignment,
    int64_t size,
    int64_t newSize) {
  if (newSize <= 0) {
    return nullptr;
  }
  auto block = aligned_alloc(alignment, newSize);
  if (block) {
    memcpy(block, p, std::min(size, newSize));
    std::free(p);
  }
  return block;
}

void MemoryAllocator::free(void* FOLLY_NULLABLE p, int64_t /* size */) {
  std::free(p);
}

MemoryPool::MemoryPool(
    const std::string& name,
    std::shared_ptr<MemoryPool> parent)
    : name_(name), parent_(std::move(parent)) {}

MemoryPool::~MemoryPool() {
  VELOX_CHECK(children_.empty());
  if (parent_ != nullptr) {
    parent_->dropChild(this);
  }
}

const std::string& MemoryPool::name() const {
  return name_;
}

MemoryPool* MemoryPool::parent() const {
  return parent_.get();
}

uint64_t MemoryPool::getChildCount() const {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  return children_.size();
}

void MemoryPool::visitChildren(
    std::function<void(MemoryPool* FOLLY_NONNULL)> visitor) const {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  for (const auto& child : children_) {
    visitor(child);
  }
}

std::shared_ptr<MemoryPool> MemoryPool::addChild(
    const std::string& name,
    int64_t cap) {
  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  // Upon name collision we would throw and not modify the map.
  auto child = genChild(shared_from_this(), name, cap);
  if (isMemoryCapped()) {
    child->capMemoryAllocation();
  }
  if (auto usageTracker = getMemoryUsageTracker()) {
    child->setMemoryUsageTracker(usageTracker->addChild());
  }
  children_.emplace_back(child.get());
  return child;
}

void MemoryPool::dropChild(const MemoryPool* FOLLY_NONNULL child) {
  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  // Implicitly synchronized in dtor of child so it's impossible for
  // MemoryManager to access after destruction of child.
  auto iter = std::find_if(
      children_.begin(), children_.end(), [child](const MemoryPool* e) {
        return e == child;
      });
  VELOX_CHECK(iter != children_.end());
  children_.erase(iter);
}

size_t MemoryPool::getPreferredSize(size_t size) {
  if (size < 8) {
    return 8;
  }
  int32_t bits = 63 - bits::countLeadingZeros(size);
  size_t lower = 1ULL << bits;
  // Size is a power of 2.
  if (lower == size) {
    return size;
  }
  // If size is below 1.5 * previous power of two, return 1.5 *
  // the previous power of two, else the next power of 2.
  if (lower + (lower / 2) >= size) {
    return lower + (lower / 2);
  }
  return lower * 2;
}

IMemoryManager& getProcessDefaultMemoryManager() {
  if (FLAGS_use_mmap_allocator_for_memory_pool) {
    return MemoryManager<MmapMemoryAllocator>::getProcessDefaultManager();
  }
  return MemoryManager<MemoryAllocator>::getProcessDefaultManager();
}

std::shared_ptr<MemoryPool> getDefaultMemoryPool(int64_t cap) {
  auto& memoryManager = getProcessDefaultMemoryManager();
  return memoryManager.getChild(cap);
}
} // namespace memory
} // namespace velox
} // namespace facebook
