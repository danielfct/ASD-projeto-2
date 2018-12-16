echo off

::set /p clientInitialDelay="Client initial delay: "
::set /p clientsNr="Number of clients: "
::set /p operationsNr="Number of operations: "
::set /p percentageOfWrites="Percentage of writes: "
::set /p replicasNr="Number of replicas: "
::set /p multiple="Multiple consoles (yes/no)? "
set clientInitialDelay=30000
set clientsNr=5
set operationsNr=10
set percentageOfWrites=100
set replicasNr=3
set multiple=yes

setlocal enabledelayedexpansion

set /a applications
set /a multipaxos
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set applications=!applications! 127.0.0.1:!port!/user/application%%i
	set multipaxos=!multipaxos! 127.0.0.1:!port!/user/application%%i/stateMachine%%i/multipaxos%%i
)

start sbt "runMain pt.unl.fct.asd.client.Tester %clientInitialDelay% %clientsNr% %operationsNr% %percentageOfWrites% 127.0.0.1 5150%applications%"

for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	if "%multiple%" == "yes" (
		start sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%multipaxos%"
	) else (
		start /B sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%multipaxos%"
	)
)

endlocal
