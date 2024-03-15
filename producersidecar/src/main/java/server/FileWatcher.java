package server;


import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileWatcher implements Runnable {
    private Path folderPath;
    private volatile boolean running = true; // Control flag
    private SharedStatus status;

    public FileWatcher(String directoryPath, SharedStatus status) {
        this.folderPath = Paths.get(directoryPath);
        this.status = status;
    }

    @Override
    public void run() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            folderPath.register(watchService, ENTRY_MODIFY);
            System.out.println("Monitoring directory for changes...");

            while (running) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException x) {
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == ENTRY_MODIFY) {
                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path modifiedPath = ev.context();
                        System.out.println(modifiedPath + " was modified.");

                        // Update the status field or perform any action as needed
                        status.changed = true;
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException ex) {
            System.err.println("Error watching the directory.");
        }
    }

    public void stopWatching() {
        running = false;
    }
}