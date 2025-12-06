#pragma once
class SpinLock {
    private:
        std::atomic<bool> lock_ = {0};
    public:
        SpinLock() {};
        ~SpinLock() {};
        void lock() noexcept{
            for(;;) {
                if(!lock_.exchange(true, std::memory_order_acquire)) {
                    return;
                }
                while(lock_.load(std::memory_order_relaxed)) {
                    __builtin_ia32_pause();    // spin
                }
            }
        }

        bool try_lock() noexcept {
            return !lock_.load(std::memory_order_acquire) && !lock_.exchange(true, std::memory_order_acquire);
        }

        void unlock() noexcept {
            lock_.store(false, std::memory_order_release);
        }
};