use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};

use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    forest::ForestImpl,
    metadata::{
        JobParams,
        JobStatus,
        SeqNo,
        TableConfig,
        TableName,
    },
    requires_docker,
    runs::{
        RunID,
        Stats,
        StatsV1,
    },
    storage::MockObjectStoreTrait,
    test_utils::setup_test_db,
};

#[tokio::test]
#[ignore] // Long-running test, ignore by default
async fn test_high_load_wal_and_compaction() {
    requires_docker!();
    println!("Starting high-load WAL and compaction test...");

    let metadata_store = setup_test_db().await.expect("Failed to setup test DB");

    // Create mock object store (no snapshot to load)
    let mock_object_store = MockObjectStoreTrait::new();
    // No expectations set since get_latest_snapshot returns None for snapshot_id
    let object_store = Arc::new(mock_object_store);

    // Create a test table
    let table_name = TableName::from(format!("test_table_{}", Uuid::new_v4()));
    let table_config = TableConfig {
        table_id: None,
        table_name: table_name.clone(),
    };

    metadata_store
        .create_table(table_config)
        .await
        .expect("Failed to create test table");

    // Get the created table to extract table_id
    let created_table = metadata_store
        .get_table(table_name.clone())
        .await
        .expect("Failed to get created table");
    let table_id = created_table.table_id.expect("Table should have an ID");

    println!("Created table: {table_name} with ID: {table_id}");

    // Shared state for coordination
    let run_counter = Arc::new(AtomicU64::new(0));
    let compaction_counter = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    const TARGET_COMPACTIONS: u64 = 100_000;
    const NUM_WRITERS: usize = 1;
    const NUM_COMPACTORS: usize = 1;

    let start_time = Instant::now();

    // Spawn writer threads
    let mut writer_handles = Vec::new();
    for writer_id in 0..NUM_WRITERS {
        let store = metadata_store.clone();
        let counter = run_counter.clone();
        let stop = stop_flag.clone();

        let handle = tokio::spawn(async move {
            let mut local_runs = Vec::new();
            let mut batch_size = 0;

            while !stop.load(Ordering::Relaxed) {
                // Generate a batch of runs
                for _ in 0..10 {
                    // Batch writes for better performance
                    let run_id = RunID::from(format!("run_{writer_id}_{}", counter.fetch_add(1, Ordering::Relaxed)));
                    let stats = Stats::StatsV1(StatsV1 {
                        min_key: format!("key_{batch_size}"),
                        max_key: format!("key_{}", batch_size + 100),
                        size_bytes: 1000 + batch_size as u64,
                        put_count: 50 + batch_size as u64,
                        delete_count: 5 + batch_size as u64,
                    });
                    local_runs.push((run_id.clone(), stats));
                    batch_size += 1;
                }

                // Append WAL batch
                match store.append_wal(local_runs.clone()).await {
                    Ok(_) => {
                        local_runs.clear();
                        batch_size = 0;
                    },
                    Err(e) => {
                        eprintln!("Writer {writer_id} failed to append WAL: {e}");
                        sleep(Duration::from_millis(10)).await;
                    },
                }

                // Small delay to prevent overwhelming the system
                sleep(Duration::from_millis(250)).await;
            }

            println!("Writer {writer_id} finished");
        });

        writer_handles.push(handle);
    }

    // Spawn compactor threads
    let mut compactor_handles = Vec::new();
    for compactor_id in 0..NUM_COMPACTORS {
        let store = metadata_store.clone();
        let object_store = object_store.clone();
        let compaction_counter = compaction_counter.clone();
        let stop = stop_flag.clone();

        let handle = tokio::spawn(async move {
            let mut last_seq_no = None;
            let forest = ForestImpl::watch(store.clone(), object_store.clone(), |stream| stream).await.unwrap();
            while !stop.load(Ordering::Relaxed) {
                let state = forest.get_state();
                if let Some(last_seq_no) = last_seq_no {
                    if last_seq_no > state.seq_no {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }

                // Use same logic as wal_compaction.rs: take up to 16 WAL runs
                let count = std::cmp::min(16, state.wal.len());
                if count == 0 {
                    // No WAL runs to compact, wait a bit
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }

                let runs_to_compact: Vec<RunID> = state
                    .wal
                    .values()
                    .take(count)
                    .map(|metadata| metadata.id.clone())
                    .collect();

                // Generate new table runs from compaction
                let mut new_table_runs = Vec::new();
                let compaction_seq = compaction_counter.fetch_add(1, Ordering::Relaxed);

                // Create fewer output runs than input (simulating compaction)
                let output_runs = std::cmp::max(1, runs_to_compact.len() / 3);
                for i in 0..output_runs {
                    let new_run_id = RunID::from(format!("compacted_{compactor_id}_{compaction_seq}_run_{i}"));
                    let combined_stats = Stats::StatsV1(StatsV1 {
                        min_key: format!("compacted_min_{compaction_seq}"),
                        max_key: format!("compacted_max_{compaction_seq}"),
                        size_bytes: (runs_to_compact.len() as u64) * 1000, // Combined size
                        put_count: (runs_to_compact.len() as u64) * 50,
                        delete_count: (runs_to_compact.len() as u64) * 5,
                    });
                    new_table_runs.push((new_run_id, table_id, combined_stats));
                }

                // Perform the compaction directly without scheduling a job
                match store
                    .append_wal_compaction(None, runs_to_compact.clone(), new_table_runs)
                    .await
                {
                    Ok(seq_no) => {
                        last_seq_no = Some(seq_no);
                        let current_compactions = compaction_counter.load(Ordering::Relaxed);
                        if current_compactions % 10000 == 0 {
                            println!(
                                "Compactor {compactor_id} completed {compaction_seq} compactions (total: \
                                 {current_compactions})"
                            );
                        }

                        if current_compactions >= TARGET_COMPACTIONS {
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
                    },
                    Err(e) => {
                        eprintln!("Compactor {compactor_id} failed compaction {compaction_seq}: {e}");
                        sleep(Duration::from_millis(100)).await;
                    },
                }
            }

            println!("Compactor {compactor_id} finished");
        });

        compactor_handles.push(handle);
    }

    // Monitor progress
    let monitor_handle = {
        let compaction_counter = compaction_counter.clone();
        let run_counter = run_counter.clone();
        let stop = stop_flag.clone();

        tokio::spawn(async move {
            let mut last_compactions = 0u64;
            let mut last_runs = 0u64;
            let mut last_time = Instant::now();

            while !stop.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(30)).await;

                let current_compactions = compaction_counter.load(Ordering::Relaxed);
                let current_runs = run_counter.load(Ordering::Relaxed);
                let current_time = Instant::now();

                let compaction_rate =
                    (current_compactions - last_compactions) as f64 / (current_time - last_time).as_secs_f64();
                let run_rate = (current_runs - last_runs) as f64 / (current_time - last_time).as_secs_f64();

                println!(
                    "Progress: {current_compactions} compactions ({compaction_rate:.1}/sec), {current_runs} runs \
                     written ({run_rate:.1}/sec)"
                );

                last_compactions = current_compactions;
                last_runs = current_runs;
                last_time = current_time;
            }
        })
    };

    // Wait for compactors to finish
    for handle in compactor_handles {
        handle.await.expect("Compactor thread panicked");
    }

    // Stop writers
    stop_flag.store(true, Ordering::Relaxed);
    for handle in writer_handles {
        handle.await.expect("Writer thread panicked");
    }

    // Stop monitor
    monitor_handle.abort();

    let total_time = start_time.elapsed();
    let final_compactions = compaction_counter.load(Ordering::Relaxed);
    let final_runs = run_counter.load(Ordering::Relaxed);

    println!("Test completed!");
    println!("Total time: {:.2}s", total_time.as_secs_f64());
    println!("Final compactions: {final_compactions}");
    println!("Final runs written: {final_runs}");
    println!(
        "Compaction rate: {:.1}/sec",
        final_compactions as f64 / total_time.as_secs_f64()
    );
    println!(
        "Run write rate: {:.1}/sec",
        final_runs as f64 / total_time.as_secs_f64()
    );

    // Verify the system is still consistent
    println!("Verifying system consistency...");

    // Check that we can still read from the database
    let tables = metadata_store
        .list_tables()
        .await
        .expect("Failed to list tables after test");
    assert!(!tables.is_empty(), "Should have at least our test table");

    // Check that we can still get changelog
    let changelog = metadata_store
        .get_changelog(SeqNo::zero())
        .await
        .expect("Failed to get changelog after test");
    assert!(!changelog.is_empty(), "Should have changelog entries");

    // Check that we can still schedule jobs
    let job_id = metadata_store
        .schedule_job(JobParams::WALCompaction)
        .await
        .expect("Failed to schedule job after test");
    let job = metadata_store
        .get_job(job_id)
        .await
        .expect("Failed to get job after test");
    assert!(matches!(job.status, JobStatus::Pending));

    println!("System consistency verified!");

    // Assert we achieved our target
    assert!(
        final_compactions >= TARGET_COMPACTIONS,
        "Should have completed at least {TARGET_COMPACTIONS} compactions, got {final_compactions}"
    );

    println!("High-load WAL and compaction test completed successfully!");
}