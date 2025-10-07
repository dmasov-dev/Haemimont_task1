package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DistributedFS API
 * Tests the distributed file system functionality
 */
public class DistributedFSTest {
    
    @Test
    void testDistributedFSFileLength() {
        // Test the DistributedFS.getInstance().fileLength() functionality
        DistributedFS dfs = DistributedFS.getInstance();
        
        // Get the file length
        long fileLength = dfs.fileLength();
        
        // Print the result as requested
        System.out.println("File length: " + fileLength + " bytes");
        
        // Assert that file length is reasonable
        assertTrue(fileLength > 0, "File length should be positive");
        assertTrue(fileLength < 1000000, "File length should be reasonable");
        
        // Print using the exact format requested
        System.out.println(DistributedFS.getInstance().fileLength());
    }
    
    @Test
    void testDistributedFSGetData() {
        // Test the getData functionality
        DistributedFS dfs = DistributedFS.getInstance();
        
        // Test getting data from offset 0
        try (var inputStream = dfs.getData(0)) {
            assertNotNull(inputStream, "InputStream should not be null");
            
            // Read some data to verify it works
            byte[] buffer = new byte[100];
            int bytesRead = inputStream.read(buffer);
            
            assertTrue(bytesRead > 0, "Should be able to read data from the file");
            System.out.println("Read " + bytesRead + " bytes from offset 0");
        } catch (Exception e) {
            fail("Should not throw exception when reading data: " + e.getMessage());
        }
    }
    
    @Test
    void testDistributedFSMultipleCalls() {
        // Test multiple calls to fileLength()
        DistributedFS dfs = DistributedFS.getInstance();
        
        long length1 = dfs.fileLength();
        long length2 = dfs.fileLength();
        
        // Should return the same length
        assertEquals(length1, length2, "Multiple calls should return same file length");
        
        System.out.println("First call: " + length1);
        System.out.println("Second call: " + length2);
    }
    
    @Test
    void testDistributedFSGetDataAtDifferentOffsets() {
        // Test getting data at different offsets
        DistributedFS dfs = DistributedFS.getInstance();
        long fileLength = dfs.fileLength();
        
        System.out.println("Testing data access at different offsets for file length: " + fileLength);
        
        // Test at beginning
        try (var inputStream = dfs.getData(0)) {
            assertNotNull(inputStream, "Should be able to get data at offset 0");
        } catch (Exception e) {
            fail("Should not throw exception when getting data at offset 0: " + e.getMessage());
        }
        
        // Test at middle if file is large enough
        if (fileLength > 10) {
            long middleOffset = fileLength / 2;
            try (var inputStream = dfs.getData(middleOffset)) {
                assertNotNull(inputStream, "Should be able to get data at middle offset");
            } catch (Exception e) {
                fail("Should not throw exception when getting data at middle offset: " + e.getMessage());
            }
        }
        
        // Test at end (should return empty stream)
        try (var inputStream = dfs.getData(fileLength)) {
            assertNotNull(inputStream, "Should be able to get data at end offset");
            // Should be empty or very small
            byte[] buffer = new byte[10];
            int bytesRead = inputStream.read(buffer);
            assertTrue(bytesRead <= 0, "Reading at end should return no data");
        } catch (Exception e) {
            fail("Should not throw exception when getting data at end offset: " + e.getMessage());
        }
    }
}
