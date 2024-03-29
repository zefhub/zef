*** Settings ***
Resource    replkeywords.resource
Resource     connect.resource
Library    RequestsLibrary

*** Keywords ***
Graph ${uid} should not be found
    Run Keyword And Expect Error    *Unknown graph*    Graph ${uid} should be found

Graph ${uid} should be found
    Eval Repl     Graph("${uid}")

Tag ${uid} should not be found
    Run Keyword And Expect Error    Tag not found    Tag ${uid} should be found

Tag ${uid} should be found
    ${ret}=    Eval Repl     lookup_uid("${uid}")
    Log    Tag lookup gave ${ret}
    Should Be True    $ret != "None"    Tag not found


Prepare Big Actions
    [Arguments]    ${num}=40    ${g}=g
    Ensure Variable Exists  ${g}
    Send REPL     actions = [{ET.Machine: {RT.Name: str(i), RT.Const: 42}} for i in range(${num})]
    Send REPL     eff = actions | transact[${g}]


Run Actions
    Send Repl   g = Graph()
    Send Repl   actions | g | run
        
    


Ensure Big Actions Takes ${duration} Seconds
    Send Repl   g = Graph()
    Prepare Big Actions
    Send Repl   eff | run
    ${dt}=      Get Last Dt
    Should Be True  ${dt} > ${duration}
    

Ensure Variable Exists
    [Arguments]     ${var}
    Send Repl   ${var} in globals()



Create Graph To Share
    [Return]    ${uid}
    Send Repl   g = Graph(True)
    ${uid}=     Eval Repl   uid(g)
    Set Test Variable    \${SHARED_UID}    ${uid}


Load Graph Across Repls
    [Arguments]    ${uid}=${SHARED_UID}
    Send All Repls       g = Graph("${SHARED_UID}")


Ensure Tag Doesnt Exist
    [Arguments]    ${tag}
    ${result}=    Eval Repl    lookup_uid("${tag}")
    Log   Result is "${result}"
    IF    $result != "None"
        Send Repl    _temp_g = Graph("${tag}")
        Send Repl    _temp_g | untag["${tag}"] | run
    END
    
    
Get Everyone Token List
    Set Default Connect Params

    ${response}=    Post To ZefHub REST    as_guest=True    data={"msg_type": "token", "msg_version": 1, "action": "list", "protocol_type": "ZEFDB", "protocol_version": 5}    

    RETURN    ${response}
    