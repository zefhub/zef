*** Settings ***
Documentation    All of the low level REPL running keywords
Library    REPLRunner.py

*** Keywords ***
Ensure Variable Exists
    [Arguments]     ${var}
    Send Repl   ${var} in globals()

Start ${num} Repls
    FOR     ${ind}      IN RANGE        ${num}
        Start Repl
    END


Send All Repls
    [Arguments]     ${cmd}
    ${repls}=   Get All Repls
    FOR   ${repl}    IN    @{repls}
        Send Repl   ${repl}     ${cmd}
    END

Send All Repls No Wait
    [Arguments]     ${cmd}
    ${repls}=   Get All Repls
    FOR   ${repl}    IN    @{repls}
        Send Repl No Wait   ${repl}     ${cmd}
    END

Wait All Repls
    ${repls}=   Get All Repls
    FOR   ${repl}    IN    @{repls}
        Wait Repl   ${repl}
    END
    
    
Run Keyword All Repls
    [Arguments]     @{args}
    ${repls}=   Get All Repls
    FOR   ${repl}    IN    @{repls}
        Switch Repl     ${repl}
        Run Keyword     @{args}
    END