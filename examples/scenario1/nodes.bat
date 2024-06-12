@echo off
set TARGET_SCRIPT=%~f1
set BROKER_WIZARD_CONFIG_PATH=%~dp0config\nodes.yaml
FOR /L %%G IN (1,1,5) DO (
    REM Your code here
    call:run_node %%G
)
exit /b 0

:run_node {
    set BROKER_WIZARD_CONFIG_KEY=node-%1
    set window_title=broker node-%1
    echo start node %BROKER_WIZARD_CONFIG_KEY%
    echo on
    start cmd /k title %window_title% ^& python %TARGET_SCRIPT% ^& pause
    @echo off
    exit /b 0
}