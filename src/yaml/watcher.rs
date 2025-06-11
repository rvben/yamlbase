use notify::RecursiveMode;
use notify_debouncer_mini::new_debouncer;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct FileWatcher {
    path: PathBuf,
    tx: mpsc::Sender<()>,
}

impl FileWatcher {
    pub fn new(path: PathBuf) -> (Self, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(10);

        let watcher = Self { path, tx };
        (watcher, rx)
    }

    pub fn start(self) -> anyhow::Result<()> {
        let path = self.path.clone();
        let tx = self.tx.clone();

        std::thread::spawn(move || {
            if let Err(e) = watch_file(path, tx) {
                error!("File watcher error: {}", e);
            }
        });

        Ok(())
    }
}

fn watch_file(path: PathBuf, tx: mpsc::Sender<()>) -> anyhow::Result<()> {
    let (tx_debounced, rx_debounced) = std::sync::mpsc::channel();

    let mut debouncer = new_debouncer(Duration::from_secs(1), tx_debounced)?;

    debouncer
        .watcher()
        .watch(&path, RecursiveMode::NonRecursive)?;

    info!("Watching for changes to: {}", path.display());

    for event in rx_debounced {
        match event {
            Ok(events) => {
                for e in events {
                    if e.path == path {
                        info!("File changed, triggering reload");
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let _ = tx.send(()).await;
                        });
                    }
                }
            }
            Err(e) => error!("Watch error: {:?}", e),
        }
    }

    Ok(())
}
