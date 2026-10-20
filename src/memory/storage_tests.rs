use super::*;

#[test]
fn blocking_bridge_runs_the_future_to_completion() {
    let blocking = Blocking::new().unwrap();

    let value = blocking.run(async { 3 + 4 }).unwrap();

    assert_eq!(value, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_bridge_can_be_called_from_a_tokio_worker() {
    let blocking = Blocking::new().unwrap();

    let value = blocking
        .run(async {
            tokio::task::yield_now().await;
            42
        })
        .unwrap();

    assert_eq!(value, 42);
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_bridge_rejects_current_thread_runtime_without_deadlocking() {
    let blocking = Blocking::new().unwrap();

    let error = blocking.run(async { 42 }).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("cannot run on a current-thread Tokio runtime")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_bridge_keeps_caller_runtime_available_to_driver_work() {
    let blocking = Blocking::new().unwrap();
    let caller_task = tokio::spawn(async {
        tokio::task::yield_now().await;
        42
    });

    let value = blocking
        .run_async(async move { caller_task.await.unwrap() })
        .await
        .unwrap();

    assert_eq!(value, 42);
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_runtime_drives_timer_and_loopback_io_futures() {
    let blocking = Blocking::new().unwrap();

    let result = blocking
        .run_async(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let client = tokio::net::TcpStream::connect(address);
            let (accepted, connected) = tokio::join!(listener.accept(), client);
            accepted?;
            connected?;
            Ok::<_, std::io::Error>(())
        })
        .await
        .unwrap();

    result.unwrap();
}

#[test]
fn blocking_bridge_reports_a_future_panic() {
    let blocking = Blocking::new().unwrap();

    let error = blocking
        .run(async { panic!("bridge test panic") })
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("storage call stopped unexpectedly")
    );
}

#[test]
fn blocking_bridge_rejects_reentrant_calls_instead_of_deadlocking() {
    let blocking = Blocking::new().unwrap();
    let nested = blocking.clone();

    let error = blocking
        .run(async move { nested.run(async {}).unwrap_err() })
        .unwrap();

    assert!(
        error
            .to_string()
            .contains("called back into its own blocking bridge")
    );
}
