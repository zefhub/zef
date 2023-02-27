*** Keywords ***
Get Env Var With Default
    [Arguments]    ${var}    ${default}=None

    ${output}=    Evaluate    import os; os.environ.get("${var}", ${default})
    RETURN   ${output}

Get Env Var
    [Arguments]    ${var}

    TRY
        ${output}=    Evaluate    os.environ["${var}"]   os
        RETURN   ${output}
    EXCEPT
        Fatal Error    "Unable to get env var ${var} so aborting"
    END