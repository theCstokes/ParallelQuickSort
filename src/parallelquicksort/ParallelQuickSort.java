/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package parallelquicksort;
 
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Phaser;
import java.util.concurrent.RecursiveAction;

/**
 *
 * @author Chris
 */
public class ParallelQuickSort extends RecursiveAction {
    /*************Synchronization Barrier Variable*************/ 
    Phaser phaser;
    
    /*************Data Array Variable*************/ 
    int[] arr = null;
    
    /*************Sharing Variables*************/ 
    int left;
    int right;
    /***********************************/ 
    /*************Main Code*************/ 
    /**
     * Used for initialization. 
     * @return
     */
    ParallelQuickSort(Phaser phaser, int[] arr) {
        this(phaser, arr, 0, arr.length - 1);
    }
 
    /**
     * Loads the corresponding values into the correct thread when called to help with thread independency.
     * @return
     */
    ParallelQuickSort(Phaser phaser, int[] arr, int left, int right) {
        this.phaser = phaser;
        this.arr = arr;
        this.left = left;
        this.right = right;
        phaser.register();  //important
    }
 
    /**
     * Thread for sorting from the right 
     * @return
     */ 
    private ParallelQuickSort leftSorter(int pivotI) {
        return new ParallelQuickSort(phaser, arr, left, --pivotI);
    }
 
    /**
     * Thread for sorting from the left  
     * @return
     */ 
    private ParallelQuickSort rightSorter(int pivotI) {
        return new ParallelQuickSort(phaser, arr, pivotI, right);
    }
    
     /**
     * Implemented thread for smaller sets of data   
     * @return
     */    
    private void recurSort(int leftI, int rightI) {
        if (rightI - leftI > 7) {
            int pIdx = partition(leftI, rightI, getPivot(arr, leftI, rightI));
            recurSort(leftI, pIdx - 1);
            recurSort(pIdx, rightI);
        } else if (rightI - leftI > 0) {
            insertion(leftI, rightI);
        }
    }
 
    //Recursive section of the code executed in the fork as 2 parallel threads
 
    @Override
    protected void compute() {
        if (right - left > 10000) {   //arbitrary number based on the appearance of speed increases.
            int pIdx = partition(left, right, getPivot(arr, left, right));
            leftSorter(pIdx).fork();
            rightSorter(pIdx).fork();
        }
        
        //for large data sets comment the following 6 lines out            
        else if (right - left > 7) {  // less than 1000 sort in this thread for stability
            recurSort(left, right);
 
        } else if (right - left > 0) {  //if less than 7 try simple insertion sort
            insertion(left, right);
        }
 
        if (isBase()) { //if this instance is the that started the sort process, wait for others to complete.
            phaser.arriveAndAwaitAdvance();
        } else {  // all not base one just arrive and de-register not waiting for others.
            phaser.arriveAndDeregister();
        }
    }
 
    /**
     * Partition the array segment based on the pivot   
     * @return
     */
    @SuppressWarnings("empty-statement")
    private int partition(int startI, int endI, int pivot) {
        for (int si = startI - 1, ei = endI + 1; ;) {
            for (; arr[++si] < pivot;) ;
            for (; ei > startI && arr[--ei] > pivot ; ) ;
            if (si >= ei) {
                return si;
            }
            swap(si, ei);
        }
    }
 
    /**
     * Insertion parts of the array segments to free up threads   
     * @return
     */
    private void insertion(int leftI, int rightI) {
        for (int i = leftI; i < rightI + 1; i++)
            for (int j = i; j > leftI && arr[j - 1] > arr[j]; j--)
                swap(j, j - 1);
 
    }
    
    /**
     * Elements that need to be swapped to other tread controlling region are moved here
     * @return
     */
    private void swap(int startI, int endI) {
        int temp = arr[startI];
        arr[startI] = arr[endI];
        arr[endI] = temp;
    }
 
    /**
     * Check to see if this instance is the first one used to sort the array.
     * @return
     */
    private boolean isBase() {
        return arr.length == (right - left) + 1;
    }
 
    /**
     * Determents the point to thread around
     * @return
     */
    private int getPivot(int[] arr, int startI, int endI) {
        int len = (endI - startI) + 1;
        // Choose a partition element, v
        int m = startI + (len >> 1);       // Small arrays
        if (len > 7) {
            int l = startI;
            int n = startI + len - 1;
            if (len > 40) {        // Big arrays
                int s = len / 8;
                l = altercation(arr, l, l + s, l + 2 * s);
                m = altercation(arr, m - s, m, m + s);
                n = altercation(arr, n - 2 * s, n - s, n);
            }
            m = altercation(arr, l, m, n); // Mid-size, med of 3
        }
        int v = arr[m];
        return v;
    }

    /**
     * Makes the comparisons of the array elements for pivot picking 
     * @return
     */
    private static int altercation(int x[], int a, int b, int c) {
        return (x[a] < x[b] ?
                (x[b] < x[c] ? b : x[a] < x[c] ? c : a) :
                (x[b] > x[c] ? b : x[a] > x[c] ? c : a));
    }
    
    /**
     * Controlling main void
     * @return
     */
    public static void main(String[] args) throws InterruptedException {
        int[] data = new int[100_000_000];
        Random r = new Random();
        for(int fill = 0; fill < data.length; ++fill){
            data[fill] = r.nextInt();
        } 
        
        ForkJoinPool pool = new ForkJoinPool();

        System.out.println("Starting");
        Phaser phaser = new Phaser();
        long startTime = System.nanoTime();
        pool.invoke(new ParallelQuickSort(phaser, data));
        long endTime = System.nanoTime();
        System.out.println("Finished");
        System.out.println("Time to run = " + (endTime - startTime) + " nanoSec");        
    }
}

