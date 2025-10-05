package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RemoteExecutor API - API for running a function on a remote machine
 * 
 * This test demonstrates the RemoteExecutor<T> class functionality:
 * - run(int serviceId, Supplier<T> func) method
 * - Valid service ID range (1-100)
 * - Error handling for invalid service IDs
 * - Basic remote execution functionality
 */
public class RemoteExecutorTest {
    
    @Test
    void testRemoteExecutorBasicFunctionality() {
        // Test the RemoteExecutor API as specified
        RemoteExecutor<String> executor = new RemoteExecutor<>();
        
        // Test basic remote execution
        String result = executor.run(1, () -> "Hello from remote machine!");
        assertEquals("Hello from remote machine!", result);
    }
    
    @Test
    void testRemoteExecutorWithInteger() {
        RemoteExecutor<Integer> executor = new RemoteExecutor<>();
        
        // Test remote execution with calculation
        Integer result = executor.run(2, () -> 10 + 20);
        assertEquals(30, result);
    }
    
    @Test
    void testValidServiceIdRange() {
        RemoteExecutor<String> executor = new RemoteExecutor<>();
        
        // Test minimum valid service ID (1)
        String result1 = executor.run(1, () -> "Service 1");
        assertEquals("Service 1", result1);
        
        // Test maximum valid service ID (100)
        String result100 = executor.run(100, () -> "Service 100");
        assertEquals("Service 100", result100);
    }
    
    @Test
    void testInvalidServiceIdHandling() {
        RemoteExecutor<String> executor = new RemoteExecutor<>();
        
        // Test invalid service ID 0
        assertThrows(IllegalArgumentException.class, () -> {
            executor.run(0, () -> "Should not execute");
        });
        
        // Test invalid service ID > 100
        assertThrows(IllegalArgumentException.class, () -> {
            executor.run(101, () -> "Should not execute");
        });
        
        // Test negative service ID
        assertThrows(IllegalArgumentException.class, () -> {
            executor.run(-1, () -> "Should not execute");
        });
    }
    
    @Test
    void testRemoteExecutionWithException() {
        RemoteExecutor<String> executor = new RemoteExecutor<>();
        
        // Test that exceptions in the supplier are properly propagated
        assertThrows(RuntimeException.class, () -> {
            executor.run(1, () -> {
                throw new RuntimeException("Remote execution failed");
            });
        });
    }
    
    @Test
    void testRemoteExecutionWithNullResult() {
        RemoteExecutor<String> executor = new RemoteExecutor<>();
        
        // Test that null results are handled properly
        String result = executor.run(1, () -> null);
        assertNull(result);
    }
}