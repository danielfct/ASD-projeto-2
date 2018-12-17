echo off

::set /p clientInitialDelay="Client initial delay: "
set /p clientsNr="Number of clients: "
::set /p operationsNr="Number of operations: "
::set /p percentageOfWrites="Percentage of writes: "
set /p replicasNr="Number of replicas: "
::set /p multiple="Multiple consoles (yes/no)? "
set clientInitialDelay=60000
::set clientsNr=5
set operationsNr=1000
set percentageOfWrites=50
::set replicasNr=4
set multiple=no

setlocal enabledelayedexpansion

set /a applications
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set applications=!applications! 127.0.0.1:!port!/user/application%%i
)

start sbt "runMain pt.unl.fct.asd.client.Tester %clientInitialDelay% %clientsNr% %operationsNr% %percentageOfWrites% 127.0.0.1 5150%applications%"

endlocal
