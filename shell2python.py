import sys,os
import subprocess


"""
#################################################################
#            STEP1 : ASSIGN PARAMETERS                          #
#################################################################
"""

print("\n===============Pyhton Script Execution Statrted=============== \n")
argv_count=len(sys.argv)-1
if argv_count == 3 or argv_count == 4:
    pass
else:
    print("You have passed {}  parameters--->Please pass 3 parameters notify_sys_level main_script_arg child_script_arg SYS_LEVEL(Optional)".format(argv_count))
    print("\n===============Pyhton Script Execution Ended=============== \n")
    exit(1)

notify_sys_level=sys.argv[1].upper()
main_script_arg=sys.argv[2].upper()
child_script_arg=sys.argv[3].upper()
if argv_count == 4:
    SYS_LEVEL=sys.argv[4].upper()
else:
    SYS_LEVEL=""



if (SYS_LEVEL=="PROD"):
    master_dir="P1"
    etl_dir="P2"
    archive_dir="P3"
    kms_key="P4"
    PWV_APP_ID="P5"
    PWV_QUERY_STRING="P6"
    PWV_QUERY_STRING_SQOOP="P7"
    PWV_QUERY_STRING_NCF="P8"

elif (SYS_LEVEL=="TEST"):
    master_dir="T1"
    etl_dir="T2"
    archive_dir="T3"
    kms_key="T4"
    PWV_APP_ID="T5"
    PWV_QUERY_STRING="T6"
    PWV_QUERY_STRING_SQOOP="T7"
    PWV_QUERY_STRING_NCF="T8"

else:
    master_dir="1"
    etl_dir="2"
    archive_dir="3"
    kms_key="4"
    PWV_APP_ID="5"
    PWV_QUERY_STRING="6"
    PWV_QUERY_STRING_SQOOP="7"
    PWV_QUERY_STRING_NCF="8"





print("\ncaflp_log: main_script_arg is : {} \ncaflp_log: child_script_arg is : {}".format(main_script_arg,child_script_arg))



def exit_shell(exit_status_code,exit_status):
    print("\ncaflp_log: ================exit_shell function===============")
    print(exit_status)
    print("caflp_log: Script Execution Ended. \n")
    print("\n===============Pyhton Script Execution Ended=============== \n")
    exit(exit_status_code)


"""
#################################################################
#            STEP2 : INVOKE MAIN SCRIPT                         #
#################################################################
"""

print("\ncaflp_log: ================Invoking Main Script===============")


def shell_script_execution(run_script_cmd):
    run_script=subprocess.Popen([run_script_cmd],shell=True)
    while(True):
        if run_script.poll() is None:
            pass
        else:
            break
    return run_script.poll()



if main_script_arg == "PRD_EMERGE":
    print("\ncaflp_log: {} are Started ".format(main_script_arg))
    # run_script_cmd="sh /home/hadoop/cafl/scripts/prd_cashflow_emerge.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
    #     .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
    #     PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_cmd="sh /home/hduser/atom_projects/sample.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
        .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
        PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_status=shell_script_execution(run_script_cmd)
    if (run_script_status==0):
        exit_shell(0,"caflp_log: PRD Cashflow completed.")
    else:
        exit_shell(1,"caflp_log: PRD Cashflow failed.")

elif main_script_arg == "PRD_SQOOP":
    print("\ncaflp_log: {} are Started ".format(main_script_arg))
    # run_script_cmd="sh /home/hadoop/cafl/scripts/prd_cashflow_sqoop.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
    #     .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
    #     PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_cmd="sh /home/hduser/atom_projects/sample.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
        .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
        PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_status=shell_script_execution(run_script_cmd)
    if (run_script_status==0):
        exit_shell(0,"caflp_sqoop_log: PRD Cashflow Sqoop completed.")
    else:
        exit_shell(1,"caflp_sqoop_log: PRD Cashflow Sqoop failed.")


elif main_script_arg == "NCF":
    print("\ncaflp_log: {} are Started ".format(main_script_arg))
    # run_script_cmd="sh /home/hadoop/cafl/scripts/ncf_emerge.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
    #     .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
    #     PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_cmd="sh /home/hduser/atom_projects/sample.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}"\
        .format(master_dir,etl_dir,archive_dir,kms_key,notify_sys_level,main_script_arg,child_script_arg,\
        PWV_APP_ID,PWV_QUERY_STRING,PWV_QUERY_STRING_SQOOP,PWV_QUERY_STRING_NCF)

    run_script_status=shell_script_execution(run_script_cmd)
    if (run_script_status==0):
        exit_shell(0,"ncf_log: NCF load completed.")
    else:
        exit_shell(1,"ncf_log: NCF load failed.")


else:
    exit_shell(1,"caflp_log: PRD Cashflow failed.")
