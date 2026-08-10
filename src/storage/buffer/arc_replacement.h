#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <list>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include "page_key.h"

class ARCReplacement {

 private:
    int t1_target_ = 0;
    int total_frame_count_;
    std::uint64_t b1_hits_ = 0;
    std::uint64_t b2_hits_ = 0;
    std::uint64_t cold_misses_ = 0;

    enum class ArcList {
        T1,
        T2,
        B1,
        B2,
    };

    struct ArcEntry {
        ArcList list;
        std::list<PageKey>::iterator it;
        std::optional<int> frame_id;
    };
    
    std::list<PageKey> t1_;
    std::list<PageKey> t2_;
    std::list<PageKey> b1_;
    std::list<PageKey> b2_;
    std::unordered_map<PageKey, ArcEntry, PageKeyHash> entries_;

    size_t residentSize() const {
        return t1_.size() + t2_.size();
    }

    static int ghostListAdjustment(size_t numerator, size_t denominator) {
        if (denominator == 0) {
            return 1;
        }
        return std::max(1, static_cast<int>(numerator / denominator));
    }

    void moveEntry(ArcEntry& entry, std::list<PageKey>& from,
                   std::list<PageKey>& to, ArcList destination) {
        // std::list::splice preserves the iterator, so entry.it remains valid.
        to.splice(to.end(), from, entry.it);
        entry.list = destination;
    }

    void promoteGhostEntryToT2(ArcEntry& entry, const PageKey& key,
                               std::list<PageKey>& ghost_list, int frame_id) {
        if (ghost_list.empty()) {
            t2_.push_back(key);
            entry.it = std::prev(t2_.end());
            entry.list = ArcList::T2;
            entry.frame_id = frame_id;
            return;
        }
        moveEntry(entry, ghost_list, t2_, ArcList::T2);
        entry.frame_id = frame_id;
    }

    void removeLru(std::list<PageKey>& list) {
        const PageKey key = list.front();
        list.pop_front();
        entries_.erase(key);
    }

    void trimGhostListForNewPage() {
        const size_t capacity = static_cast<size_t>(total_frame_count_);
        const size_t recent_size = t1_.size() + b1_.size();
        const size_t total_size =
            recent_size + t2_.size() + b2_.size();

        if (recent_size == capacity) {
            // |T1| + |B1| <= total_frame_count_
            // make sure we don't cache old old data on B1.
            if (!b1_.empty()) {
                removeLru(b1_);
            }
        } else if (recent_size < capacity &&
                   total_size >= 2 * capacity) {
            // |T1| + |T2| + |B1| + |B2| <= 2*total_frame_count_
            if (!b2_.empty()) {
                removeLru(b2_);
            }
        }
    }

    std::optional<int> findEvictable(
        const std::list<PageKey>& list,
        const std::function<bool(int)>& is_evictable) const {
        for (const PageKey& key : list) {
            const ArcEntry& entry = entries_.at(key);
            if (entry.frame_id.has_value() &&
                is_evictable(*entry.frame_id)) {
                return entry.frame_id;
            }
        }
        return std::nullopt;
    }

  public:

    size_t t1Size() const { return t1_.size(); }
    size_t t2Size() const { return t2_.size(); }
    size_t b1Size() const { return b1_.size(); }
    size_t b2Size() const { return b2_.size(); }
    int t1Target() const { return t1_target_; }
    std::uint64_t b1Hits() const { return b1_hits_; }
    std::uint64_t b2Hits() const { return b2_hits_; }
    std::uint64_t coldMisses() const { return cold_misses_; }

    void reset() {
        t1_target_ = 0;
        b1_hits_ = 0;
        b2_hits_ = 0;
        cold_misses_ = 0;
        t1_.clear();
        t2_.clear();
        b1_.clear();
        b2_.clear();
        entries_.clear();
    }

    explicit ARCReplacement(int total_frame_count)
        : total_frame_count_(total_frame_count) {
        if (total_frame_count_ <= 0) {
            throw std::invalid_argument(
                "ARCReplacement requires a positive frame count.");
        }
    }

    void onEviction(int victim_frame_id, const PageKey& key){
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if (!entry.frame_id.has_value() ||
                *entry.frame_id != victim_frame_id) {
                throw std::logic_error("frame ID does not match ARC entry.");
            }
            if(entry.list == ArcList::T1){
                moveEntry(entry, t1_, b1_, ArcList::B1);
                entry.frame_id = std::nullopt;
                return;
            }else if(entry.list == ArcList::T2){
                moveEntry(entry, t2_, b2_, ArcList::B2);
                entry.frame_id = std::nullopt;
                return;
            }
        }
        throw std::logic_error("should not be called.");
    }

    void onResidentPageAccess(const PageKey& key){
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if(entry.list == ArcList::T1){
                moveEntry(entry, t1_, t2_, ArcList::T2);
                return;
            }else if(entry.list == ArcList::T2){
                moveEntry(entry, t2_, t2_, ArcList::T2);
                return;
            }
        }
        throw std::logic_error("This method should not be called for none-resident pages.");
    }

    /**
     * Record and classify a non-resident request before selecting a victim.
     * ARC must adapt t1_target_ before applying its REPLACE decision.
     */
    void onNonResidentPageRequest(const PageKey& key) {
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            ++cold_misses_;
            return;
        }

        const ArcEntry& entry = it->second;
        if (entry.list == ArcList::B1) {
            ++b1_hits_;
            const int adjustment =
                ghostListAdjustment(b2_.size(), b1_.size());
            t1_target_ = std::min(
                total_frame_count_, t1_target_ + adjustment);
            return;
        }
        if (entry.list == ArcList::B2) {
            ++b2_hits_;
            const int adjustment =
                ghostListAdjustment(b1_.size(), b2_.size());
            t1_target_ = std::max(0, t1_target_ - adjustment);
            return;
        }

        throw std::logic_error(
            "This method should not be called for resident pages.");
    }

    /**
     * Call this only when there is no free frame.
     */
    std::optional<int> selectVictimFrame(
        const PageKey& key,
        const std::function<bool(int)>& is_evictable) const {
        auto it = entries_.find(key);
        if (it != entries_.end() &&
            (it->second.list == ArcList::T1 ||
             it->second.list == ArcList::T2)) {
            throw std::logic_error(
                "This method should not be called for resident pages.");
        }

        const bool b2_hit =
            it != entries_.end() && it->second.list == ArcList::B2;
        const bool prefer_t1 =
            !t1_.empty() &&
            (t1_.size() > static_cast<size_t>(t1_target_) ||
             (b2_hit &&
              t1_.size() == static_cast<size_t>(t1_target_)));

        if (prefer_t1) {
            auto victim = findEvictable(t1_, is_evictable);
            if (victim.has_value()) {
                return victim;
            }
            return findEvictable(t2_, is_evictable);
        }

        auto victim = findEvictable(t2_, is_evictable);
        if (victim.has_value()) {
            return victim;
        }
        return findEvictable(t1_, is_evictable);
    }

    /**
     * A free frame should exist before calling this method.
     */
    void onNoneResidentPageAccess(int frame_id, const PageKey& key){
        if (residentSize() >= static_cast<size_t>(total_frame_count_)) {
            throw std::logic_error(
                "onNoneResidentPageAccess requires a free frame.");
        }

        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if(entry.list == ArcList::B1){
                promoteGhostEntryToT2(entry, key, b1_, frame_id);
                return;
            }else if(entry.list == ArcList::B2){
                promoteGhostEntryToT2(entry, key, b2_, frame_id);
                return;
            }
            throw std::logic_error(
                "This method should not be called for resident pages.");
        }else{
            trimGhostListForNewPage();
            // add page to T1.
            t1_.push_back(key);
            ArcEntry entry;
            entry.list = ArcList::T1;
            entry.it = std::prev(t1_.end());
            entry.frame_id = frame_id;
            entries_[key] = entry;
        }
    }
};
