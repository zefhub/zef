*** Settings ***
Documentation     General zef tests
Resource     graphkeywords.resource

*** Test Cases ***

Test Long Enough
    [Tags]      test-long-enough
    Start Repl
    Ensure Big Actions Takes 5 Seconds

Testing big merge with loading graph
    Start 10 Repls

    Switch Repl     0
    Create Graph To Share
    Send Repl    g | release_transactor_role | run

    Load Graph Across Repls
    Run Keyword All Repls    Prepare Big Actions

    Send All Repls No Wait   eff | run

    Log    Between sending and waiting

    Wait All Repls

    Send All Repls    g | sync | run
    # Give the repls one more second to tidy up messages so I can prevent
    # spurious errors from zefhub for timeouts etc
    Sleep    2    Sleeping to prevent ZefHub timeout errors
    

Testing multiple my repl
    ${ind1}=    Start Repl
    ${ind2}=    Start Repl
    Log    The indices are ${ind1} and ${ind2}    DEBUG

    Send Repl    ${ind1}    g = Graph()
    ${uid}=       Eval Repl    ${ind1}    str(uid(g))
    Log    The uid is ${uid}    DEBUG

    Switch REPL    ${ind1}
    Graph ${uid} should be found
    Switch REPL    ${ind2}
    Graph ${uid} should not be found

    Send Repl    ${ind1}    g | sync | run

    Switch REPL    ${ind1}
    Graph ${uid} should be found
    Switch REPL    ${ind2}
    Graph ${uid} should be found

    Quit Repl    ${ind1}
    Quit Repl    ${ind2}
    