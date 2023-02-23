# Write crontab expressions for pipeline scheduler

You can use cron expressions to schedule FHIR Analytics pipeline jobs. FHIR Analytics pipeline is using [NCrontab](https://github.com/atifaziz/NCrontab) tp oarse the cron expressions.

The cron expression the pipeline currently suported is a string consisting of 6 fields, separated by spaces. Each field represents a particular unit of time and specifies when the job should be run. The format for a cron expression is six-part as follows:

    * * * * * *
    - - - - - -
    | | | | | |
    | | | | | +--- day of week (0 - 6) (Sunday=0)
    | | | | +----- month (1 - 12)
    | | | +------- day of month (1 - 31)
    | | +--------- hour (0 - 23)
    | +----------- min (0 - 59)
    +------------- sec (0 - 59)

The fields are as follows:

- Second: The second of the minute (0-59)
- Minute: The minute of the hour (0-59)
- Hour: The hour of the day (0-23)
- Day of the month: The day of the month (1-31)
- Month: The month of the year (1-12)
- Day of the week: The day of the week (0-6), where Sunday is 0 and Saturday is 6.

**_NOTE:_** You **can not** omit the *Second* field in cron expression when configuring the pipeline, although other applications may allow this. 

Here are some examples:

- Run every 10 minutes: ___* /10 * * * *___
- Run every 30 minutes: ___* /30 * * * *___  
- Run daily at 0am: ___* * * * * *___
- Run at 10:00am every Saturday:  ___0 0 10 * * 6___ 


