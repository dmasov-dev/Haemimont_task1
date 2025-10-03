package org.example;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.nio.charset.StandardCharsets;


interface DistributedFS {
    long fileLength();
    InputStream getData(long offset);
    static DistributedFS getInstance() {
        // Implementation provided by the environment (MOCK at end of file)
        return DistributedFSImpl.getInstance(); 
    }
}


class RemoteExecutor<T> {
    public T run(int serviceId, Supplier<T> func) {
        // Implementation provided by the environment (MOCK at end of file)
        if (serviceId < 1 || serviceId > 100) {
            throw new IllegalArgumentException("Invalid serviceId: " + serviceId);
        }
        
        try {
            Thread.sleep(100); // Simulate 100ms startup time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        return func.get();
    }
}



public class DistributedWordCount {
    
    // Total number of available compute nodes (1 to 100 inclusive)
    private static final int NUM_NODES = 100;
    
    // The local thread pool should match the number of remote nodes to maximize parallelism
    private static final int MAX_LOCAL_THREADS = NUM_NODES; 
    
    private final DistributedFS distributedFS;
    private final ExecutorService executorService;
    
    public DistributedWordCount() {
        // Instantiate the (real or mock) DFS
        this.distributedFS = DistributedFS.getInstance(); 
        // Thread pool to manage the local threads calling the remote executor
        this.executorService = Executors.newFixedThreadPool(MAX_LOCAL_THREADS);
    }
    

    public int findMaxWordsPerLine() {
        try {
            // Blocking call (100 ms on first call)
            long fileLength = distributedFS.fileLength();
            
            // Calculate partition size to utilize all 100 nodes
            long partitionSize = fileLength / NUM_NODES;
            
            // --- 1. Dispatch Tasks ---
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            
            for (int i = 0; i < NUM_NODES; i++) {
                final int nodeId = i + 1; // Service IDs 1 to 100
                
                // Calculate byte offsets for this node
                final long startOffset = (long) i * partitionSize;
                // Last node takes all remaining bytes to ensure no data is lost
                final long endOffset = (i == NUM_NODES - 1) ? fileLength : startOffset + partitionSize;
                final long length = endOffset - startOffset;
                
                // Submit the remote execution call to the local thread pool
                CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                    // This blocks the local thread until the remote node returns a result
                    return processPartition(nodeId, startOffset, length, fileLength);
                }, executorService);
                
                futures.add(future);
            }
            
            // --- 2. Aggregate Results (Reduce Phase) ---
            int maxWords = 0;
            for (CompletableFuture<Integer> future : futures) {
                try {
                    int partitionMax = future.get(); // Blocks until a result is available
                    maxWords = Math.max(maxWords, partitionMax);
                } catch (InterruptedException | ExecutionException e) {
                    // Log the error but continue aggregation (soft failure tolerance)
                    System.err.println("Error processing partition: " + e.getMessage());
                }
            }
            
            return maxWords;
            
        } catch (Exception e) {
            throw new RuntimeException("Error finding max words per line", e);
        } finally {
            executorService.shutdown();
        }
    }

    private int processPartition(int nodeId, long startOffset, long length, long fileLength) {
        // RemoteExecutor runs the Supplier (lambda) on the remote machine
        return new RemoteExecutor<Integer>().run(nodeId, () -> {
            
            long maxWordsInChunk = 0;
            long readOffset = startOffset;
            
            // --- CRITICAL BOUNDARY HANDLING ---
            // If this is NOT the first partition (startOffset > 0), 
            // we must advance the readOffset past the first partial line.
            if (startOffset > 0 && startOffset < fileLength) {
                try {
                    // This helper method finds the first newline and returns the byte offset after it.
                    readOffset = findNextLineStart(distributedFS, startOffset, fileLength);
                } catch (IOException e) {
                    System.err.println("Node " + nodeId + " failed boundary check: " + e.getMessage());
                    return 0;
                }
            }
            
            
            long bytesToRead = (startOffset + length > fileLength) ? fileLength - readOffset : (startOffset + length) - readOffset;

            if (bytesToRead <= 0) {
                 return 0; // Nothing left to read in this partition
            }
            
            // Get the stream starting from the corrected readOffset
            try (InputStream is = distributedFS.getData(readOffset)) {
                
                InputStream limitedStream = new ByteArrayInputStream(
                    is.readNBytes((int) bytesToRead), 0, (int) bytesToRead
                );

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(limitedStream, StandardCharsets.US_ASCII))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        int wordCount = countWordsInLine(line);
                        maxWordsInChunk = Math.max(maxWordsInChunk, wordCount);
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading chunk data on node " + nodeId, e);
            }
            
            return (int) maxWordsInChunk;
        });
    }

    private long findNextLineStart(DistributedFS dfs, long offset, long totalLength) throws IOException {
        long currentReadPos = offset;
        
        // We only check for a newline in the immediate vicinity of the boundary
        try (InputStream is = dfs.getData(offset)) {
            // We use a small, fixed buffer to find the first line break efficiently
            byte[] buffer = new byte[1024]; 
            int bytesRead;

            // Read small chunks until we find a newline or hit EOF
            while ((bytesRead = is.read(buffer, 0, (int) Math.min(buffer.length, totalLength - currentReadPos))) != -1) {
                for (int i = 0; i < bytesRead; i++) {
                    if (buffer[i] == '\n' || buffer[i] == '\r') {
                        // Return the position *after* the newline character
                        return currentReadPos + i + 1; 
                    }
                }
                currentReadPos += bytesRead;
                if (currentReadPos >= totalLength) break;
            }
        }
        
        return offset; 
    }
    

    private int countWordsInLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return 0;
        }
        
        // Split by one or more whitespace characters (\s+) and filter out empty strings
        return (int) Arrays.stream(line.trim().split("\\s+"))
                .filter(word -> !word.isEmpty())
                .count();
    }
    
    public static void main(String[] args) {
        try {
            // Set up a mock instance that will be used by DistributedFS.getInstance()
            // The file content is mocked at the bottom of this file.
            DistributedWordCount processor = new DistributedWordCount();
            long startTime = System.currentTimeMillis();
            
            int maxWords = processor.findMaxWordsPerLine();
            
            long endTime = System.currentTimeMillis();
            System.out.println("Maximum words per line: " + maxWords);
            System.out.println("Total Processing time: " + (endTime - startTime) + " ms");
            
        } catch (Exception e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}


// Mock implementation of DistributedFS (Overriding the provided interface)
class DistributedFSImpl implements DistributedFS {
    private static DistributedFS instance;
    private final byte[] mockData;
    
    private DistributedFSImpl() {
        // MOCK DATA: 4 partitions of content. Max words = 8
        // Split positions: approx 30 bytes, 60 bytes, 90 bytes
        String content = 
            "word1 word2 word3\n" + // 3 words
            "a b c d e f g h\n" + // 8 words <--- MAX
            "This line is cut here and spans to the next part.\n" + // 8 words + newline
            "part 2 starts here, but the previous line was partial. \n" + // 7 words
            "The very end line has only a few words left."; // 7 words
            
        this.mockData = content.getBytes(StandardCharsets.US_ASCII);
    }
    
    public static DistributedFS getInstance() {
        if (instance == null) {
            instance = new DistributedFSImpl();
        }
        return instance;
    }
    
    @Override
    public long fileLength() {
        // Simulate 100ms delay on first call
        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return mockData.length;
    }
    
    @Override
    public InputStream getData(long offset) {
        // Simulate 100ms delay
        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        
        int start = (int) offset;
        if (start >= mockData.length) {
             return new ByteArrayInputStream(new byte[0]);
        }
        
        // Return a stream of the data from the offset to the end
        return new ByteArrayInputStream(mockData, start, mockData.length - start);
    }
}


