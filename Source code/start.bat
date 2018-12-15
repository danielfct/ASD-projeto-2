echo off

::set /p clientsNr="Number of clients? "
::set /p operationsNr="Number of operations? "
::set /p replicasNr="Number of replicas? "
::set /p multiple="Multiple consoles (yes/no)? "
set clientsNr=1
set operationsNr=10
set replicasNr=3
set multiple=yes

setlocal enabledelayedexpansion

set /a applications
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set applications=!applications! 127.0.0.1:!port!/user/application%%i
)
set /a stateMachines
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set stateMachines=!stateMachines! 127.0.0.1:!port!/user/application%%i/stateMachine%%i
)

start sbt "runMain pt.unl.fct.asd.client.Client %clientsNr% %operationsNr% 127.0.0.1 5150%applications%"

for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	if "%multiple%" == "yes" (
		start sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%stateMachines%"
	) else (
		start /B sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%stateMachines%"
	)
)


endlocal
