package minio

val api1: Signature & Synchronization = new 
        Structure with 
        Interpreter with 
        Fibers with 
        Synchronization {}

val api2: Signature & Synchronization & Timing = new 
        Direct with 
        Fibers with 
        Timing with
        Synchronization {}

val api3: Signature & Synchronization & Timing = new 
        Simple with 
        Fibers with 
        Timing with
        Synchronization {}