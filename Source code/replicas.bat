echo off

set /p replicasNr="Number of replicas: "
::set /p multiple="Multiple consoles (yes/no)? "
::set replicasNr=4
set multiple=no

setlocal enabledelayedexpansion

set /a multipaxos
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set multipaxos=!multipaxos! 127.0.0.1:!port!/user/application%%i/stateMachine%%i/multipaxos%%i
)

for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	if "%multiple%" == "yes" (
		start sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%multipaxos%"
	) else (
		start /B sbt "runMain pt.unl.fct.asd.server.Application %%i 127.0.0.1 !port!%multipaxos%"
	)
)

endlocal