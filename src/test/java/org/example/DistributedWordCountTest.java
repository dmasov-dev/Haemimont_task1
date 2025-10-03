package org.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit tests for DistributedWordCount system
 */
public class DistributedWordCountTest {
    
    private DistributedWordCount processor;
    
    @BeforeEach
    void setUp() {
        processor = new DistributedWordCount();
    }
    
    @Test
    void testFindMaxWordsPerLine() {
        // Test basic functionality
        int result = processor.findMaxWordsPerLine();
        
        // Should return a positive number
        assertTrue(result > 0, "Result should be positive");
        
        // Should be a reasonable number (not too high)
        assertTrue(result < 50000, "Result should be reasonable");
    }
    
    @Test
    void testConcurrentExecution() throws InterruptedException {
        // Test concurrent execution
        Thread[] threads = new Thread[3];
        int[] results = new int[3];
        
        for (int i = 0; i < 3; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                DistributedWordCount p = new DistributedWordCount();
                results[index] = p.findMaxWordsPerLine();
            });
            threads[i].start();
        }
        
        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }
        
        // All results should be the same
        for (int i = 1; i < results.length; i++) {
            assertEquals(results[0], results[i], 
                "Concurrent executions should return same result");
        }
    }
    
    @Test
    void testWordCountEdgeCases() {
        // Test word counting logic with edge cases
        assertEquals(1, countWords("SingleWord"));
        assertEquals(2, countWords("Two words"));
        assertEquals(2, countWords("   Leading spaces   "));
        assertEquals(2, countWords("Trailing spaces   "));
        assertEquals(4, countWords("Multiple   spaces   between   words"));
        assertEquals(0, countWords(""));
        assertEquals(0, countWords("   "));
    }
    
    private int countWords(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }
        
        String[] words = text.trim().split("\\s+");
        int count = 0;
        for (String word : words) {
            if (!word.isEmpty()) {
                count++;
            }
        }
        return count;
    }
}
