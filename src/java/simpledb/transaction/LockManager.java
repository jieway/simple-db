package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private final Map<PageId , List<Lock>> lockMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    public boolean tryAcquireLock(PageId pageId, TransactionId tid,
                                  int lockType, int timeout) {
        long now = System.currentTimeMillis();
        while (true) {
            // 防止死锁 lab4 ex5
            if (System.currentTimeMillis() - now >= timeout) return false;
            if (acquireLock(pageId , tid, lockType)) return true;
        }
    }

    private synchronized boolean acquireLock(PageId pageId, TransactionId tid, int lockType) {
        // 待获取的 page 没有锁
        if (!this.lockMap.containsKey(pageId)) {
            Lock lock = new Lock(tid , lockType);
            List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            this.lockMap.put(pageId , locks);
            return true;
        }

        // 有锁的情况
        List<Lock> locks = this.lockMap.get(pageId);

        // 判断锁是不是自己之前拿过的
        for (Lock lock : locks) {
            if (lock.getTid().equals(tid)) {
                if (lock.getLockType() == lockType) {
                    return true;
                }
                // 已经有写锁了
                if (lock.getLockType() == 1) {
                    return true;
                }
                // 已有读锁，想要写锁，还要保证页号对应的页面只有一个锁才能升级
                if (lock.getLockType() == 0 && locks.size() == 1) {
                    lock.setLockType(1);
                    return true;
                }
                return false;
            }
        }

        // tid 不一致，检查是否存在写锁
        if (locks.size() > 0 && locks.get(0).getLockType() == 1) {
            return false;
        }

        // 想要读锁
        if (lockType == 0) {
            final Lock lock = new Lock(tid, lockType);
            locks.add(lock);
            return true;
        }
        return false;
    }


    public synchronized boolean releaseLock(PageId pageId, TransactionId tid) {
        if (!this.lockMap.containsKey(pageId)) {
            return false;
        }
        List<Lock> locks = this.lockMap.get(pageId);
        for (Lock lock : locks) {
            if (lock.getTid().equals(tid)) {
                locks.remove(lock);
                this.lockMap.put(pageId, locks);
                if (locks.size() == 0) {
                    this.lockMap.remove(pageId);
                }
                return true;
            }
        }
        return false;
    }

    public synchronized void releaseLockByTxn(final TransactionId tid) {
        this.lockMap.forEach((pid, locks) -> {
            if (holdsLock(pid, tid)) {
                releaseLock(pid, tid);
            }
        });
    }

    public synchronized boolean holdsLock(PageId pageId, TransactionId tid) {
        if (!this.lockMap.containsKey(pageId)) {
            return false;
        }
        List<Lock> locks = this.lockMap.get(pageId);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
