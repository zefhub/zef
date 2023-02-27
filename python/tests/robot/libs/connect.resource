*** Settings ***
Resource     general_utils.resource

*** Keywords ***

Set Default Connect Params
    ${ZEFHUB_URL}=    Get Env Var    ZEFHUB_URL
    ${ZEFHUB_AUTH_KEY}=    Get Env Var    ZEFHUB_AUTH_KEY

    ${zefhub_rest_url}=   Evaluate    "${ZEFHUB_URL}".replace("ws", "http") + "/REST"

    &{default_headers}=    Create Dictionary    x-auth-token=${ZEFHUB_AUTH_KEY}
    &{default_headers}=    Set Suite Variable   ${default_headers}
    &{zefhub_url}=    Set Suite Variable    ${ZEFHUB_URL}
    &{zefhub_rest_url}=    Set Suite Variable    ${zefhub_rest_url}


Post To ZefHub REST
    [Arguments]    ${data}    ${as_guest}=False
    IF   ${as_guest}
        ${response}=    POST    ${zefhub_rest_url}   expected_status=200    data=${data}
    ELSE
        ${response}=    POST    ${zefhub_rest_url}   expected_status=200    data=${data}    headers=${default_headers}
    END
    RETURN    ${response}


Wait For ZefHub To Start
    FOR    ${index}    IN RANGE    10
        ${success}=    Run Keyword And Return Status    Query Health Check
        Return From Keyword If    ${success}
        SLEEP    2s
    END
    Fatal Error    Zefhub did not restart


Query Health Check
    ${base_url}=    Evaluate    $zefhub_rest_url[:-len("/REST")]
    GET    ${base_url}/_healthy   expected_status=200
    