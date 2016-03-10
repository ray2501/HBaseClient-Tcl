# HBaseClient --
#
#	HBase REST Client Library for Tcl
#
# Copyright (C) 2016 Danilo Chang <ray2501@gmail.com>
#
# Retcltribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Retcltributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Retcltributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#

package require Tcl 8.6
package require TclOO
package require http
package require base64
package require tdom

package provide HBaseClient 0.1


oo::class create HBaseClient {
    variable server

    constructor {{SERVER http://localhost:8080/}} {
        set server $SERVER
    }

    destructor {
    }

    method send_request {url method {headers ""} {needstate 0} {data ""}} {
        variable tok

        if {[string length $data] < 1} {
            if {[catch {set tok [http::geturl $url -method $method \
                -headers $headers]}]} {
                return "error"
            }
        } else {
            if {[catch {set tok [http::geturl $url -method $method \
                -headers $headers -query $data]}]} {
                return "error"
            }
        }

        if {$needstate != 0} {
            set res [http::status $tok]
        } else {
            set res [http::data $tok]
        }

        http::cleanup $tok
        return $res
    }


    #
    # Cluster Information
    #
    method version {} {
        set myurl "$server/version/cluster"
        set headerl [list Content-Type "text/plain"]
        set res [my send_request $myurl GET $headerl]
        return $res
    }

    method status {} {
        set myurl "$server/status/cluster"
        set headerl [list Content-Type "text/plain"]
        set res [my send_request $myurl GET $headerl]
        return $res
    }

    method listTable {} {
        set myurl "$server/"
        set headerl [list Accept "text/xml" Content-Type "text/xml"]
        set res [my send_request $myurl GET $headerl]
        set response [list]
        
        set XML $res
        set doc [dom parse $XML]
        set root [$doc documentElement]
        set nodeList [$root selectNodes /TableList/table]        
        
        foreach node $nodeList { 
            set name [$node getAttribute name]
            lappend response $name        
        }        
        
        return $response
    }


    #
    # Table Information
    #
    method getTableSchema  {tableName} {
        set myurl "$server/$tableName/schema"
        set headerl [list Accept "text/xml" Content-Type "text/xml"]
        set res [my send_request $myurl GET $headerl]
        return $res
    }

    method createTable {tableName COLUMN} {
        variable content

        set content "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        append content "<TableSchema name=\"$tableName\">"
        append content "  <ColumnSchema name=\"$COLUMN\" />"
        append content "</TableSchema>"

        set myurl "$server/$tableName/schema"
        set headerl [list Accept "text/xml" Content-Type "text/xml"]
        set res [my send_request $myurl PUT $headerl 1 $content]
        return $res
    }

    method updateTable {tableName COLUMN} {
        variable content

        set content "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        append content "<TableSchema name=\"$tableName\">"
        append content "  <ColumnSchema name=\"$COLUMN\" />"
        append content "</TableSchema>"

        set myurl "$server/$tableName/schema"
        set headerl [list Accept "text/xml" Content-Type "text/xml"]
        set res [my send_request $myurl POST $headerl 1 $content]
        return $res
    }

    method deleteTable {tableName} {
        set myurl "$server/$tableName/schema"
        set headerl [list Content-Type "application/plain"]
        set res [my send_request $myurl DELETE $headerl 1]
        return $res
    }

    method getTableInfo {tableName} {
        set myurl "$server/$tableName/regions"
        set headerl [list Accept "text/xml" Content-Type "text/xml"]
        set res [my send_request $myurl GET $headerl]
        return $res
    }

    #
    # Get and put
    #

    method getValue {tableName rowName} {
        set myurl "$server/$tableName/$rowName"
        set headerl [list Accept "text/xml"]
        set res [my send_request $myurl GET $headerl]

        set response [list]

        if {[string compare -nocase -length 9 $res {Not found}] == 0} {
            lappend response $res
            return $res
        } elseif {[string compare -nocase -length 5 $res {error}] == 0} {
            lappend response $res
            return $res
        }

        set response [list]
        set XML $res
        set doc [dom parse $XML]
        set root [$doc documentElement]
        set node [$root selectNodes /CellSet/Row]
        set rowname [$node getAttribute key]
        set rowname [::base64::decode $rowname]
        lappend response $rowname

        set nodeList [$root selectNodes /CellSet/Row/Cell]
        foreach node $nodeList { 
            set column_list [list]
            set column [$node getAttribute column]
            set column [::base64::decode $column]
            lappend column_list $column
            set timestamp [$node getAttribute timestamp]
            lappend column_list $timestamp
            set value [[$node firstChild] data]
            set value [::base64::decode $value]
            lappend column_list $value
            lappend response $column_list 
        }

        return $response
    }

    method putValue {tableName rowName colName qualifier value} {
        set content "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        set row_encode [::base64::encode $rowName]
        append content "<CellSet><Row key=\"$row_encode\">"
        append content "<Cell "

        set column "$colName:$qualifier"
        set column_encode [::base64::encode $column]
        append content "column=\"$column_encode\">"

        set value_encode  [::base64::encode $value]
        append content "$value_encode"

        append content "</Cell></Row></CellSet>";

        set myurl "$server/$tableName/$rowName/$colName:$qualifier"        
        set headerl [list Content-Type "text/xml"]
        set res [my send_request $myurl PUT $headerl 1 $content]
        return $res
    }

    method deleteValue {tableName rowName colName qualifier} {
        set myurl "$server/$tableName/$rowName/$colName:$qualifier"        
        set headerl [list Content-Type "application/plain"]
        set res [my send_request $myurl DELETE $headerl 1]
        return $res
    }
}
