package com.stewart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author mstewart 12/3/15
 */
public final class Cocktail {

  static final Logger logger = LoggerFactory.getLogger("com.stewart.Cocktail");

  static final SharedKafkaSender sharedKafkaSender = new SharedKafkaSender("metrics");

  static final String INPUT_GLOB = "*.txt";

  static final Boolean REPROCESS_WORKING_DIR_ON_STARTUP = Boolean.TRUE;

  static Path INPUT    = null;
  static Path CLAIMED  = null;
  static Path FAILED   = null;

  static class FileProcessor implements Runnable {
    Path path;

    public FileProcessor(Path p) {
      path = p;
    }

    @Override
    public void run() {
      try {
        // nb: set heap size >= num threads x expected file size
        byte[] content = Files.readAllBytes(path);
        if(sharedKafkaSender.send(content))
          release(path);
        else
          fail(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  static class ServiceShutdownHook extends Thread {

    ExecutorService inputService, processingService;

    public ServiceShutdownHook(ExecutorService inputService, ExecutorService processingService) {
      this.inputService = inputService;
      this.processingService = processingService;
    }

    public void run() {

      try {

        inputService.shutdown();
        inputService.awaitTermination(2, TimeUnit.SECONDS);

        processingService.shutdown();
        processingService.awaitTermination(4, TimeUnit.SECONDS);

      } catch (InterruptedException e) {
        logger.error("There was a problem shutting down the executor service. Messages may be lost as a result.", e);
      }
    }
  }

  private static Path claim(Path path) throws IOException {
    return move(path, CLAIMED);
  }

  private static void release(Path path) throws IOException {
    Files.delete(path);
  }

  private static Path move(Path path, Path destination) throws IOException {
    return Files.move(path, destination.resolve(path.getFileName()), StandardCopyOption.ATOMIC_MOVE);
  }

  private static Path fail(Path path) throws IOException {
    return Files.move(path, FAILED);
  }

  private static void cleanup() {
    if (REPROCESS_WORKING_DIR_ON_STARTUP) {
      logger.debug("Moving previously in-process files back to incoming directory.");
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(CLAIMED, INPUT_GLOB)) {
        stream.forEach(path -> {
          try {
            move(path, INPUT);
          } catch (IOException e) {
            logger.error("Unable to reclaim file for sending", e);
          }
        });
      } catch (IOException e) {
        logger.error(e.getMessage());
      }

      logger.info("Clean up complete.\nSystem ready.");

    }
  }

  private static Path workingDirectory(String var) {

    if (var == null) { throw new IllegalStateException("INPUT, CLAIMED, and FAILED env vars must be defined."); }

    Path path = Paths.get(var);
    if(!Files.isDirectory(path) || !Files.isReadable(path) || !Files.isWritable(path)) {
      throw new IllegalStateException
              ("INPUT, CLAIMED, and FAILED env vars must be readable and writable directories.");
    }

    return path;
  }

  private static void setup() {
    Map<String, String> env = System.getenv();

    INPUT   = workingDirectory(env.get("INPUT"));
    CLAIMED = workingDirectory(env.get("CLAIMED"));
    FAILED  = workingDirectory(env.get("FAILED"));
  }

  public static void main(String[] args) throws IOException {

    setup();

    ScheduledExecutorService fsScannerExecutor = Executors.newScheduledThreadPool(1);
    ExecutorService fileProcessorExecutor = Executors.newFixedThreadPool(4);

    Runtime.getRuntime().addShutdownHook(new ServiceShutdownHook(fsScannerExecutor, fileProcessorExecutor));

    cleanup();

    Runnable scanTask = () -> {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(INPUT, INPUT_GLOB)) {
        stream.forEach(path -> {
          try {
            Path claimedPath = claim(path);
            fileProcessorExecutor.submit(new FileProcessor(claimedPath));
          } catch (IOException e) {
            logger.error("Unable to claim " + path);
            e.printStackTrace();
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    };

    int initialDelay = 0, period = 1;

    fsScannerExecutor.scheduleAtFixedRate(scanTask, initialDelay, period, TimeUnit.SECONDS);

  }
}
