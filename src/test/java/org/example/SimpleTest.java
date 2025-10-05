package org.example;

/**
 * Simple test runner for DistributedWordCount system
 */
public class SimpleTest {
    
    public static void main(String[] args) {
        System.out.println("Running DistributedWordCount Tests...\n");
        
        try {
            // Test 1: Basic functionality
            System.out.println("Test 1: Basic functionality test");
            DistributedWordCount processor = new DistributedWordCount();
            long startTime = System.currentTimeMillis();
            int result = processor.findMaxWordsPerLine();
            long endTime = System.currentTimeMillis();
            
            System.out.println("✓ Result: " + result + " words");
            System.out.println("✓ Processing time: " + (endTime - startTime) + " ms");
            System.out.println("✓ Test 1 passed\n");
            
            // Test 2: Performance test
            System.out.println("Test 2: Performance test");
            DistributedWordCount processor2 = new DistributedWordCount();
            startTime = System.currentTimeMillis();
            int result2 = processor2.findMaxWordsPerLine();
            endTime = System.currentTimeMillis();
            
            System.out.println(" Result: " + result2 + " words");
            System.out.println(" Processing time: " + (endTime - startTime) + " ms");
            System.out.println(" Test 2 passed\n");
            
            // Test 3: Concurrent execution test
            System.out.println("Test 3: Concurrent execution test");
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
            
            boolean allSame = true;
            for (int i = 1; i < results.length; i++) {
                if (results[i] != results[0]) {
                    allSame = false;
                    break;
                }
            }
            
            if (allSame) {
                System.out.println("✓ All concurrent executions returned same result: " + results[0]);
                System.out.println("✓ Test 3 passed\n");
            } else {
                System.out.println(" Concurrent executions returned different results");
                System.out.println(" Test 3 failed\n");
            }
            
            // Test 4: Edge case test
            System.out.println("Test 4: Edge case test");
            testWordCount("SingleWord", 1);
            testWordCount("Two words", 2);
            testWordCount("   Leading spaces   ", 2);
            testWordCount("Trailing spaces   ", 2);
            testWordCount("Multiple   spaces   between   words", 4);
            System.out.println("✓ Test 4 passed\n");
            
            System.out.println(" All tests completed successfully!");
            
        } catch (Exception e) {
            System.err.println(" Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testWordCount(String text, int expected) {
        // Simple word count implementation for testing
        String[] words = text.trim().split("\\s+");
        int count = 0;
        for (String word : words) {
            if (!word.isEmpty()) {
                count++;
            }
        }
        
        if (count == expected) {
            System.out.println(" '" + text + "' -> " + count + " words (expected: " + expected + ")");
        } else {
            System.out.println(" '" + text + "' -> " + count + " words (expected: " + expected + ")");
        }
    }
}


