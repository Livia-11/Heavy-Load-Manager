package com.example;

public class MainProgram{
    public static void main(String[] args) {
        CallMe target=new CallMe();
        Caller obj1=new Caller(target,"Hello");
        Caller obj2=new Caller(target,"synchronised");
        Caller obj3=new Caller(target,"World");
        obj1.t.start();
        obj2.t.start();
        obj3.t.start();
        try{
            obj1.t.join();
            obj1.t.join();
            obj1.t.join();
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
        }
    }

}
