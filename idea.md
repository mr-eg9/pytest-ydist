# Features
Distributed test execution.
Resource management for distributed test execution.
Compatible with other pytest hooks etc...

## Hooks
`init_worker`
`init_scheduler`
`handle_command` -- WorkerSesssion level?
`emit_event` -- WorkerSession level?
note: No hooks on scheduler level, however some flags should affect the scheduler

## Flags
`--numworkers`
`--batchsize`

## Features
 - Cancellation
 - Some flavor of work stealing?
    - Optimizing this will likely require that there is some rough heuristic on how long a test
        takes to run.
 - Out-of-band commands
    - Cancellation
    - WorkStealing
    - NewNode
