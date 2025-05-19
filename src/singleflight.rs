use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Mutex, watch};

#[derive(Error, Clone, Debug)]
pub enum SingleflightError {
    #[error("Operation in progress")]
    InProgress,
    #[error("Operation failed: {0}")]
    Failed(Arc<dyn std::error::Error + Send + Sync>),
    #[error("Operation was canceled")]
    Canceled,
}

// Type alias to simplify the complex type
type FlightMap<K, V> = Arc<Mutex<HashMap<K, watch::Sender<Result<V, SingleflightError>>>>>;

#[derive(Debug, Clone)]
pub struct Singleflight<K, V> {
    flights: FlightMap<K, V>,
}

impl<K, V> Singleflight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + Debug,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            flights: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Run work only once for a given key, sharing the result with all callers
    pub async fn run<F, Fut, E>(&self, key: K, work: F) -> Result<V, SingleflightError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Try to get a receiver for existing work or start new work
        let receiver = {
            let mut flights = self.flights.lock().await;

            if let Some(sender) = flights.get(&key) {
                // Work is already in progress, get a receiver to wait for the result
                sender.subscribe()
            } else {
                // No work in progress, set up a channel and start the work
                let (sender, receiver) = watch::channel(Err(SingleflightError::InProgress));
                flights.insert(key.clone(), sender.clone());

                // Release the lock before executing the work
                drop(flights);

                // Start the work in the background
                let flights_clone = self.flights.clone();
                let key_clone = key.clone();
                tokio::spawn(async move {
                    let result = work()
                        .await
                        .map_err(|e| SingleflightError::Failed(Arc::new(e)));

                    // Update the result and remove the entry
                    let mut flights = flights_clone.lock().await;
                    if let Some(s) = flights.get(&key_clone) {
                        let _ = s.send(result.clone());
                    }
                    flights.remove(&key_clone);
                });

                receiver
            }
        };

        // Wait for the result
        let mut receiver = receiver;
        loop {
            // Check if we have a real result yet
            match receiver.borrow().clone() {
                Err(SingleflightError::InProgress) => {},
                result => return result,
            }

            // Still in progress, wait for an update
            match receiver.changed().await {
                Ok(_) => {},
                Err(_) => {
                    return Err(SingleflightError::Canceled);
                },
            }
        }
    }

    /// Fire work in background if not already running, without waiting for the result
    pub fn fire_and_forget<F, Fut, E>(&self, key: K, work: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        let flights_clone = self.flights.clone();
        let key_clone = key.clone();

        tokio::spawn(async move {
            // Create the sender/receiver if work isn't already in progress
            let sender_opt = {
                let mut flights = flights_clone.lock().await;

                // Check if work is already in progress
                if flights.contains_key(&key_clone) {
                    // Work already running, nothing to do
                    None
                } else {
                    // No work in progress, set up a channel
                    let (sender, _) = watch::channel(Err(SingleflightError::InProgress));
                    flights.insert(key_clone.clone(), sender.clone());
                    Some(sender)
                }
            };

            // If we created a sender, do the work
            if let Some(sender) = sender_opt {
                // Execute the work (outside the lock)
                let result = work()
                    .await
                    .map_err(|e| SingleflightError::Failed(Arc::new(e)));

                // Update the result and remove entry
                let mut flights = flights_clone.lock().await;
                if flights.get(&key_clone).is_some() {
                    let _ = sender.send(result);
                    flights.remove(&key_clone);
                }
            }
        });
    }

    /// Check if work is currently in progress for a key
    pub async fn is_in_progress(&self, key: &K) -> bool {
        let flights = self.flights.lock().await;
        flights.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct TestKey(String);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestValue(String);

    // Custom error type for testing
    #[derive(Debug, thiserror::Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    #[tokio::test]
    async fn test_run_shares_result() {
        let sf = Singleflight::<TestKey, TestValue>::new();
        let execution_count = Arc::new(AtomicUsize::new(0));

        let key = TestKey("test-key".to_string());
        let expected_value = TestValue("result".to_string());

        // Function to simulate work
        let make_work = |count: Arc<AtomicUsize>| {
            let value = expected_value.clone();
            move || async move {
                count.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(50)).await;
                Ok::<_, TestError>(value)
            }
        };

        // Run multiple tasks concurrently
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let sf = sf.clone();
                let key = key.clone();
                let work = make_work(execution_count.clone());
                tokio::spawn(async move { sf.run(key, work).await })
            })
            .collect();

        // Wait for all tasks to complete
        let results = futures::future::join_all(tasks).await;

        // Check all tasks got the same result
        for result in results {
            let value = result.unwrap().unwrap();
            assert_eq!(value, expected_value);
        }

        // Check that the work was only executed once
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_fire_and_forget_deduplication() {
        let sf = Singleflight::<TestKey, TestValue>::new();
        let execution_count = Arc::new(AtomicUsize::new(0));

        let key = TestKey("fire-and-forget".to_string());

        // Function to simulate work
        let make_work = |count: Arc<AtomicUsize>| {
            move || async move {
                count.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(TestValue("done".to_string()))
            }
        };

        // Fire multiple tasks without waiting
        for _ in 0..5 {
            sf.fire_and_forget(key.clone(), make_work(execution_count.clone()));
        }

        // Wait for work to start
        sleep(Duration::from_millis(10)).await;

        // Verify work is in progress
        assert!(sf.is_in_progress(&key).await);

        // Wait for all work to complete
        sleep(Duration::from_millis(200)).await;

        // Verify work is no longer in progress
        assert!(!sf.is_in_progress(&key).await);

        // Check that the work was only executed once
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_run_propagates_errors() {
        let sf = Singleflight::<TestKey, TestValue>::new();
        let key = TestKey("error-key".to_string());

        // Function that returns an error
        let work = || async {
            sleep(Duration::from_millis(50)).await;
            Err(TestError("test failure".to_string()))
        };

        // Run and check for error
        let result = sf.run(key.clone(), work).await;

        // The result should be an error
        match result {
            Err(SingleflightError::Failed(err)) => {
                let err_str = err.to_string();
                assert!(err_str.contains("test failure"));
            },
            other => panic!("Expected Failed error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_run_with_timeout() {
        let sf = Singleflight::<TestKey, TestValue>::new();
        let key = TestKey("timeout-key".to_string());

        // Function that takes a long time
        let slow_work = || async {
            sleep(Duration::from_secs(2)).await;
            Ok::<_, TestError>(TestValue("slow result".to_string()))
        };

        // Start the slow work
        sf.fire_and_forget(key.clone(), slow_work);

        // Wait a bit to ensure the work has started
        sleep(Duration::from_millis(10)).await;

        // Now try to get the result with a timeout
        let result = timeout(
            Duration::from_millis(100),
            sf.run(key.clone(), || async {
                panic!("This should not be called because work is already in progress");
                #[allow(unreachable_code)]
                Ok::<_, TestError>(TestValue("unused".to_string()))
            }),
        )
        .await;

        // The timeout should have occurred
        assert!(result.is_err());

        // But the work should still be in progress
        assert!(sf.is_in_progress(&key).await);

        // Wait for the work to finish
        sleep(Duration::from_secs(2)).await;

        // Now the work should be complete
        assert!(!sf.is_in_progress(&key).await);
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let sf = Singleflight::<TestKey, TestValue>::new();
        let execution_count = Arc::new(AtomicUsize::new(0));

        let keys = vec![
            TestKey("key1".to_string()),
            TestKey("key2".to_string()),
            TestKey("key3".to_string()),
        ];

        // Start work for each key
        let tasks: Vec<_> = keys
            .iter()
            .map(|key| {
                let sf = sf.clone();
                let key = key.clone();
                let count = execution_count.clone();

                tokio::spawn(async move {
                    sf.run(key.clone(), || async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(50)).await;
                        Ok::<_, TestError>(TestValue(format!("result for {:?}", key)))
                    })
                    .await
                })
            })
            .collect();

        // Wait for all tasks to complete
        let results = futures::future::join_all(tasks).await;

        // Check all tasks succeeded
        for result in results {
            assert!(result.unwrap().is_ok());
        }

        // Check that work was executed once for each key
        assert_eq!(execution_count.load(Ordering::SeqCst), 3);
    }
}
