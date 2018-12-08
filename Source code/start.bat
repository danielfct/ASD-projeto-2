echo off

set /p replicasNr="Number of replicas? "
set /p multiple="Multiple consoles? "

setlocal enabledelayedexpansion

set /a replicas
for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	set replicas=!replicas! 127.0.0.1:!port!/user/stateMachine%%i
)

for /L %%i in (1, 1, %replicasNr%) Do (
	set /a "port=5150+%%i"
	if "%multiple%" == "yes" (
		start sbt "runMain StateMachine %%i 127.0.0.1 !port!%replicas%"
	) else (
		start /B sbt "runMain StateMachine %%i 127.0.0.1 !port!%replicas%"
	)
)

start sbt "runMain Client 127.0.0.1 5150!replicas!"

endlocal