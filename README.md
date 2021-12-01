# General purpose worker pool

## Example usage:

    {
        pool := NewWorkerPool(1)
        pool.Start()
        defer pool.Stop()

        go pool.Execute(
             func(data interface{}) {
    			time.Sleep(time.Duration(2) * time.Second)
             },
             "first",
             1)
    }
