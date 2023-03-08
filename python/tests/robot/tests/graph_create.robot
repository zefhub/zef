*** Settings ***
Documentation     Testing create=True options for graphs
Resource     graphkeywords.resource

*** Variables ***

${TEST_TAG}    robot-framework-testing-tag2

*** Test Cases ***

Multiple graph loading with same tag
    Start 10 Repls

    Ensure Tag Doesnt Exist    ${TEST_TAG}
    

    Send All Repls No Wait   guid,created = lookup_uid("${TEST_TAG}", create=True)
    Wait All Repls

    ${count}=    Set Variable    ${0}
    FOR     ${ind}      IN RANGE        10
        ${thisval}=   Eval Repl    ${ind}    created
        ${count}=    Set Variable If    ${thisval}    ${count + 1}    ${count}
    END

    Should Be Equal    ${count}    ${1}