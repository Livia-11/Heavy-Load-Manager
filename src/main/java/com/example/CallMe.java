package com.example;

public class CallMe {
    synchronized void call(String message){
        System.out.print("["+message);
        try{
            Thread.sleep(1000);
        }
        catch(InterruptedException e){
            System.out.println("Interrupted");
        }
        System.out.println("]");
    }

}







