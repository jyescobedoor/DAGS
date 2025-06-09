from datetime import datetime
import pendulum
from airflow import DAG
#from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.task_group import TaskGroup

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# Define the SSH connection details
ssh_conn_id = "dataStage_dsadm"

# Configuraciones del DAG principal
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1, 00, 00, 00, tzinfo=local_tz),
    'retries': 1,
}

# Definición del DAG principal
with DAG('DWH_MALLA_ODS',
         default_args=default_args,
         schedulel='00 00 * * *', ##Ejecucion de malla diaria desde las 20:00
         catchup=False,
         tags=["10.203.34.30", "Malla_ODS","FactorIT"],) as dag:

    Inicio = SSHOperator(
        task_id='inicia_malla_ods',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/mailiniciomallaods.sh ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

    # Creación grupo de tareas
    with TaskGroup("DWHODS_EXT_Catalog") as group1:  
        Task1G1 = SSHOperator(
            task_id='dwhods_ext_sys_datadict_item',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_datadict_item ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G1 = SSHOperator(
            task_id='dwhods_ext_ar_dic_channel',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_dic_channel ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G1 = SSHOperator(
            task_id='dwhods_ext_pm_classification',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_classification ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G1 = SSHOperator(
            task_id='dwhods_ext_sys_datadict_item_lang',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_datadict_item_lang ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G1 = SSHOperator(
            task_id='dwhods_ext_ar_dic_invoice_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_dic_invoice_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G1 = SSHOperator(
            task_id='dwhods_ext_pm_entity_plan',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_entity_plan ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G1 = SSHOperator(
            task_id='dwhods_ext_sys_employee',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_employee ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G1 = SSHOperator(
            task_id='dwhods_ext_ar_dic_payment_method',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_dic_payment_method ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G1 = SSHOperator(
            task_id='dwhods_ext_pm_entity_relation',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_entity_relation ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G1 = SSHOperator(
            task_id='dwhods_ext_sys_org',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_org ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G1 = SSHOperator(
            task_id='dwhods_ext_ar_dic_reason_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_dic_reason_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G1 = SSHOperator(
            task_id='dwhods_ext_pm_offering',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_offering ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G1 = SSHOperator(
            task_id='dwhods_ext_sys_property',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_property ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G1 = SSHOperator(
            task_id='dwhods_ext_ar_dic_trx_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_dic_trx_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G1 = SSHOperator(
            task_id='dwhods_ext_pm_offering_rental_ctz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_offering_rental_ctz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G1 = SSHOperator(
            task_id='dwhods_ext_sys_region',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_region ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G1 = SSHOperator(
            task_id='dwhods_ext_cm_charge_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqncm_charge_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G1 = SSHOperator(
            task_id='dwhods_ext_pm_plan',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_plan ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G1 = SSHOperator(
            task_id='dwhods_ext_sys_staff',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_staff ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G1 = SSHOperator(
            task_id='dwhods_ext_fp_logvendedores_final',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnfp_logvendedores_final ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G1 = SSHOperator(
            task_id='dwhods_ext_pm_plan_simpleprice',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_plan_simpleprice ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G1 = SSHOperator(
            task_id='dwhods_ext_t_bme_languagelocaldisplay',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bme_languagelocaldisplay ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G1 = SSHOperator(
            task_id='dwhods_ext_im_item_sku',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnim_item_sku ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G1 = SSHOperator(
            task_id='dwhods_ext_pm_product',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_product ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G1 = SSHOperator(
            task_id='dwhods_ext_t_bme_publicdatadict',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bme_publicdatadict ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G1 = SSHOperator(
            task_id='dwhods_ext_inf_addr_ref_purpose',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_addr_ref_purpose ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G1 = SSHOperator(
            task_id='dwhods_ext_pm_relationtype',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_relationtype ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G1 = SSHOperator(
            task_id='dwhods_ext_t_sr_servicerequesttype',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_sr_servicerequesttype ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G1 = SSHOperator(
            task_id='dwhods_ext_inf_bill_cycle_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_bill_cycle_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G1 = SSHOperator(
            task_id='dwhods_ext_pm_target_market_ctz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_target_market_ctz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G1 = SSHOperator(
            task_id='dwhods_ext_t_ss_option',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_ss_option ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G1 = SSHOperator(
            task_id='dwhods_ext_inf_group',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_group ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G1 = SSHOperator(
            task_id='dwhods_ext_sys_address',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_address ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G1 = SSHOperator(
            task_id='dwhods_ext_t_ss_question',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_ss_question ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G1 = SSHOperator(
            task_id='dwhods_ext_inf_group_member',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_group_member ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G1 = SSHOperator(
            task_id='dwhods_ext_sys_charge_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_charge_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task37G1 = SSHOperator(
            task_id='dwhods_ext_t_ucp_groupinfo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_ucp_groupinfo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task38G1 = SSHOperator(
            task_id='dwhods_ext_inf_organization',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_organization ',
            conn_timeout = None,
            cmd_timeout = None, 
        )
        
        #Configuración dependencias tareas grupo1
        Task1G1 >> Task4G1 >> Task7G1 >> Task10G1 >> Task13G1 >> Task16G1 >> Task19G1 >> Task22G1 >> Task25G1 >> Task28G1 >> Task31G1 >> Task34G1 >> Task37G1
        Task2G1 >> Task5G1 >> Task8G1 >> Task11G1 >> Task14G1 >> Task17G1 >> Task20G1 >> Task23G1 >> Task26G1 >> Task29G1 >> Task32G1 >> Task35G1 >> Task38G1
        Task3G1 >> Task6G1 >> Task9G1 >> Task12G1 >> Task15G1 >> Task18G1 >> Task21G1 >> Task24G1 >> Task27G1 >> Task30G1 >> Task33G1 >> Task36G1

    # Creación grupo de tareas
    with TaskGroup("DWHODS_STG_Catalog") as group2: ##El grupo2 depende de la finalizacion de las tareas del grupo1--DWHODS_EXT_Catalog
        Task1G2 = SSHOperator(
            task_id='dwhods_stg_sys_datadict_item',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_datadict_item ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_dic_channel',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_dic_channel ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G2 = SSHOperator(
            task_id='dwhods_stg_pm_classification',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_classification ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G2 = SSHOperator(
            task_id='dwhods_stg_sys_datadict_item_lang',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_datadict_item_lang ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_dic_invoice_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_dic_invoice_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_pm_entity_plan',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_entity_plan ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_employee',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_employee ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_dic_payment_method',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_dic_payment_method ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G2 = SSHOperator(
            task_id='dwhods_stg_pm_entity_relation',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_entity_relation ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_org',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_org ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_dic_reason_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_dic_reason_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G2 = SSHOperator(
            task_id='dwhods_stg_pm_offering',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_offering ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_property',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_property ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_dic_trx_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_dic_trx_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G2 = SSHOperator(
            task_id='dwhods_stg_pm_offering_rental_ctz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_offering_rental_ctz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G2 = SSHOperator(
            task_id='dwhods_stg_sys_region',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_region ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_cm_charge_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqncm_charge_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_pm_plan',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_plan ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_staff',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_staff ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_fp_logvendedores_final',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnfp_logvendedores_final ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_pm_plan_simpleprice',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_plan_simpleprice ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_bme_languagelocaldisplay',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bme_languagelocaldisplay ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_im_item_sku',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnim_item_sku ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G2 = SSHOperator(
            task_id='dwhods_stg_pm_product',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_product ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_bme_publicdatadict',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bme_publicdatadict ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G2 = SSHOperator(
            task_id='dwhods_stg_inf_addr_ref_purpose',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_addr_ref_purpose ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G2 = SSHOperator(
            task_id='dwhods_stg_pm_relationtype',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_relationtype ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_sr_servicerequesttype',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_sr_servicerequesttype ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G2 = SSHOperator(
            task_id='dwhods_stg_inf_bill_cycle_type',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_bill_cycle_type ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G2 = SSHOperator(
            task_id='dwhods_stg_pm_target_market_ctz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_target_market_ctz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_ss_option',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_ss_option ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G2 = SSHOperator(
            task_id='dwhods_stg_inf_group',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_group ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_address',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_address ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_ss_question',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_ss_question ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G2 = SSHOperator(
            task_id='dwhods_stg_inf_group_member',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_group_member ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sys_charge_code',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_charge_code ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task37G2 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_ucp_groupinfo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_ucp_groupinfo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task38G2 = SSHOperator(
            task_id='dwhods_stg_inf_organization',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_organization ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

    # Creación grupo de tareas
    with TaskGroup("DWHODS_EXT_Davox") as group3: ##El grupo3 depende de la finalizacion de las tareas del grupo2--DWHODS_STG_Catalog
        Task1G3 = SSHOperator(
            task_id='dwhods_ext_davox_distribucion_recaudo_anulado',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_distribucion_recaudo_anulado ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G3 = SSHOperator(
            task_id='dwhods_ext_davox_banco',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_banco ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G3 = SSHOperator(
            task_id='dwhods_ext_davox_factura_cuenta',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_factura_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G3 = SSHOperator(
            task_id='dwhods_ext_davox_caracteristica_cus',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_caracteristica_cus ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G3 = SSHOperator(
            task_id='dwhods_ext_davox_factura_proveedor',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_factura_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G3 = SSHOperator(
            task_id='dwhods_ext_davox_cartera_cuenta_proveedor',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cartera_cuenta_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G3 = SSHOperator(
            task_id='dwhods_ext_davox_financiacion',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G3 = SSHOperator(
            task_id='dwhods_ext_davox_causa_cargo',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_causa_cargo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G3 = SSHOperator(
            task_id='dwhods_ext_davox_impuesto_cuota_financiacion',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_impuesto_cuota_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G3 = SSHOperator(
            task_id='dwhods_ext_davox_cg_ref_codes',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cg_ref_codes ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G3 = SSHOperator(
            task_id='dwhods_ext_davox_impuesto_distribucion_reca_anu',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_impuesto_distribucion_reca_anu ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G3 = SSHOperator(
            task_id='dwhods_ext_davox_ciclo',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_ciclo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G3 = SSHOperator(
            task_id='dwhods_ext_davox_impuesto_factura_proveedor',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_impuesto_factura_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G3 = SSHOperator(
            task_id='dwhods_ext_davox_ciudad',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_ciudad ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G3 = SSHOperator(
            task_id='dwhods_ext_davox_periodo',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_periodo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G3 = SSHOperator(
            task_id='dwhods_ext_davox_cliente',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cliente ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G3 = SSHOperator(
            task_id='dwhods_ext_davox_periodo_facturacion',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_periodo_facturacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G3 = SSHOperator(
            task_id='dwhods_ext_davox_concepto',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_concepto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G3 = SSHOperator(
            task_id='dwhods_ext_davox_plan_tarifario',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_plan_tarifario ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G3 = SSHOperator(
            task_id='dwhods_ext_davox_contacto',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_contacto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G3 = SSHOperator(
            task_id='dwhods_ext_davox_proveedor',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G3 = SSHOperator(
            task_id='dwhods_ext_davox_contrato',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G3 = SSHOperator(
            task_id='dwhods_ext_davox_recaudo',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_recaudo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G3 = SSHOperator(
            task_id='dwhods_ext_davox_cuenta',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G3 = SSHOperator(
            task_id='dwhods_ext_davox_referencia',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_referencia ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G3 = SSHOperator(
            task_id='dwhods_ext_davox_cuenta_contrato',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cuenta_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G3 = SSHOperator(
            task_id='dwhods_ext_davox_region',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_region ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G3 = SSHOperator(
            task_id='dwhods_ext_davox_cuota_financiacion',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_cuota_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G3 = SSHOperator(
            task_id='dwhods_ext_davox_segmentacion_cuenta',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_segmentacion_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G3 = SSHOperator(
            task_id='dwhods_ext_davox_departamento_geografico',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_departamento_geografico ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G3 = SSHOperator(
            task_id='dwhods_ext_davox_sucursal_banco',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_sucursal_banco ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G3 = SSHOperator(
            task_id='dwhods_ext_davox_detalle_castigo_cartera',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_detalle_castigo_cartera ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G3 = SSHOperator(
            task_id='dwhods_ext_davox_telefono_x_cus',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_telefono_x_cus ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G3 = SSHOperator(
            task_id='dwhods_ext_davox_detalle_contrato',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_detalle_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G3 = SSHOperator(
            task_id='dwhods_ext_davox_tipo_impuesto',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_tipo_impuesto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G3 = SSHOperator(
            task_id='dwhods_ext_davox_detalle_tipo_impuesto',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_detalle_tipo_impuesto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task37G3 = SSHOperator(
            task_id='dwhods_ext_davox_tipo_moneda',
            command='date;hostname;sleep 60',
            ssh_conn_id=ssh_conn_id,
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_tipo_moneda ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        #Configuración dependencias tareas grupo3
        Task1G3 >> Task3G3 >> Task5G3 >> Task7G3 >> Task9G3 >> Task11G3 >> Task13G3 >> Task15G3 >> Task17G3 >> Task19G3 >> Task21G3 >> Task23G3 >> Task25G3 >> Task27G3 >> Task29G3 >> Task31G3 >> Task33G3 >> Task35G3 >> Task37G3
        Task2G3 >> Task4G3 >> Task6G3 >> Task8G3 >> Task10G3 >> Task12G3 >> Task14G3 >> Task16G3 >> Task18G3 >> Task20G3 >> Task22G3 >> Task24G3 >> Task26G3 >> Task28G3 >> Task30G3 >> Task32G3 >> Task34G3 >> Task36G3

    # Creación grupo de tareas
    with TaskGroup("DWHODS_STG_Davox") as group4: ##El grupo4 depende de la finalizacion de las tareas del grupo3--DWHODS_EXT_Davox
        Task1G4 = SSHOperator(
            task_id='dwhods_stg_davox_financiacion',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G4 = SSHOperator(
            task_id='dwhods_stg_davox_factura_proveedor',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_factura_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G4 = SSHOperator(
            task_id='dwhods_stg_davox_factura_cuenta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_factura_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G4 = SSHOperator(
            task_id='dwhods_stg_davox_detalle_tipo_impuesto',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_detalle_tipo_impuesto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G4 = SSHOperator(
            task_id='dwhods_stg_davox_tipo_moneda',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_tipo_moneda ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G4 = SSHOperator(
            task_id='dwhods_stg_davox_tipo_impuesto',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_tipo_impuesto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G4 = SSHOperator(
            task_id='dwhods_stg_davox_telefono_x_cus',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_telefono_x_cus ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G4 = SSHOperator(
            task_id='dwhods_stg_davox_sucursal_banco',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_sucursal_banco ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G4 = SSHOperator(
            task_id='dwhods_stg_davox_segmentacion_cuenta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_segmentacion_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G4 = SSHOperator(
            task_id='dwhods_stg_davox_region',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_region ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G4 = SSHOperator(
            task_id='dwhods_stg_davox_referencia',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_referencia ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G4 = SSHOperator(
            task_id='dwhods_stg_davox_recaudo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_recaudo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G4 = SSHOperator(
            task_id='dwhods_stg_davox_proveedor',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G4 = SSHOperator(
            task_id='dwhods_stg_davox_plan_tarifario',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_plan_tarifario ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G4 = SSHOperator(
            task_id='dwhods_stg_davox_periodo_facturacion',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_periodo_facturacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G4 = SSHOperator(
            task_id='dwhods_stg_davox_periodo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_prldavox_periodo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G4 = SSHOperator(
            task_id='dwhods_stg_davox_impuesto_factura_proveedor',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_impuesto_factura_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G4 = SSHOperator(
            task_id='dwhods_stg_davox_impuesto_distribucion_reca_anu',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_impuesto_distribucion_reca_anu ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G4 = SSHOperator(
            task_id='dwhods_stg_davox_impuesto_cuota_financiacion',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_impuesto_cuota_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G4 = SSHOperator(
            task_id='dwhods_stg_davox_distribucion_recaudo_anulado',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_distribucion_recaudo_anulado ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G4 = SSHOperator(
            task_id='dwhods_stg_davox_detalle_contrato',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_detalle_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G4 = SSHOperator(
            task_id='dwhods_stg_davox_detalle_castigo_cartera',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_detalle_castigo_cartera ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G4 = SSHOperator(
            task_id='dwhods_stg_davox_departamento_geografico',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_departamento_geografico ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G4 = SSHOperator(
            task_id='dwhods_stg_davox_cuota_financiacion',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cuota_financiacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G4 = SSHOperator(
            task_id='dwhods_stg_davox_cuenta_contrato',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cuenta_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G4 = SSHOperator(
            task_id='dwhods_stg_davox_cuenta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G4 = SSHOperator(
            task_id='dwhods_stg_davox_contrato',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_contrato ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G4 = SSHOperator(
            task_id='dwhods_stg_davox_contacto',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_contacto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G4 = SSHOperator(
            task_id='dwhods_stg_davox_concepto',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_concepto ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G4 = SSHOperator(
            task_id='dwhods_stg_davox_cliente',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cliente ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G4 = SSHOperator(
            task_id='dwhods_stg_davox_ciudad',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_ciudad ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G4 = SSHOperator(
            task_id='dwhods_stg_davox_ciclo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_ciclo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G4 = SSHOperator(
            task_id='dwhods_stg_davox_cg_ref_codes',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cg_ref_codes ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G4 = SSHOperator(
            task_id='dwhods_stg_davox_causa_cargo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_davox_causa_cargo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G4 = SSHOperator(
            task_id='dwhods_stg_davox_cartera_cuenta_proveedor',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_cartera_cuenta_proveedor ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G4 = SSHOperator(
            task_id='dwhods_stg_davox_caracteristica_cus',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_caracteristica_cus ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task37G4 = SSHOperator(
            task_id='dwhods_stg_davox_banco',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_banco ',
            conn_timeout = None,
            cmd_timeout = None, 
        )
 
    # Creación grupo de tareas
    with TaskGroup("DWHODS_EXT_SharedSources") as group5: ##El grupo5 depende de la finalizacion de las tareas del grupo4--DWHODS_STG_Davox

        Task1G5 = SSHOperator(
            task_id='dwhods_ext_om_order_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G5 = SSHOperator(
            task_id='dwhods_ext_t_sr_servicerequest',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_sr_servicerequest_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G5 = SSHOperator(
            task_id='dwhods_ext_ar_payment',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_payment_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G5 = SSHOperator(
            task_id='dwhods_ext_t_pbh_problemactivity',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_pbh_problemactivity_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G5 = SSHOperator(
            task_id='dwhods_ext_ar_adjustment',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_adjustment_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G5 = SSHOperator(
            task_id='dwhods_ext_t_bpm_interface_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bpm_interface_info_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G5 = SSHOperator(
            task_id='dwhods_ext_t_bpm_form_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bpm_form_info_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G5 = SSHOperator(
            task_id='dwhods_ext_om_order_item_his_vertical',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_item_his_vertical_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G5 = SSHOperator(
            task_id='dwhods_ext_inf_subs_drop_reason_ex',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_subs_drop_reason_ex_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G5 = SSHOperator(
            task_id='dwhods_ext_inf_legalization_cz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_legalization_cz_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G5 = SSHOperator(
            task_id='dwhods_ext_t_pbh_problemprocess',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_pbh_problemprocess_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G5 = SSHOperator(
            task_id='dwhods_ext_ar_apply_detail',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_apply_detail_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G5 = SSHOperator(
            task_id='dwhods_ext_om_order_line_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_line_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G5 = SSHOperator(
            task_id='dwhods_ext_inf_subscriber_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_subscriber_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G5 = SSHOperator(
            task_id='dwhods_ext_inf_offering_inst_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_offering_inst_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G5 = SSHOperator(
            task_id='dwhods_ext_t_pbh_problemworkitem',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_pbh_problemworkitem_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G5 = SSHOperator(
            task_id='dwhods_ext_ar_credit_balance',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_credit_balance_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G5 = SSHOperator(
            task_id='dwhods_ext_sr_contact_log',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsr_contact_log_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G5 = SSHOperator(
            task_id='dwhods_ext_om_order_ext_node_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_ext_node_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )
#####################Job onice en autoys
        #TaskXGX = BashOperator(
        #    task_id='dwhods_ext_inf_prod_inst_his', ###s(dwhods_ext_inf_offering_inst_his)
        #    #ssh_conn_id=ssh_conn_id,
        #    bash_command='date;hostname;sleep 60',
        #    CAFksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_prod_inst_his_period ',
        #    conn_timeout = None,
        #    cmd_timeout = None, 
        #)

        Task20G5 = SSHOperator(
            task_id='dwhods_ext_sr_reception',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsr_reception_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G5 = SSHOperator(
            task_id='dwhods_ext_ar_invoice',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_invoice_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        # Configuración del flujo del grupo de tareas
        Task1G5 >> Task8G5 >> Task13G5 >> Task18G5 >> Task20G5
        Task2G5 >> Task9G5 >> Task14G5 >> Task19G5
        Task3G5 >> Task10G5 >> Task15G5
        Task4G5 >> Task11G5 >> Task16G5
        Task5G5 >> Task12G5 >> Task17G5
    
    # Creación grupo de tareas
    with TaskGroup("DWHODS_STG_SharedSources") as group6: ##El grupo6 depende de la finalizacion de las tareas del grupo5--DWHODS_EXT_SharedSources

        Task1G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_om_order_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_sr_servicerequest',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_prlt_sr_servicerequest ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_payment',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_payment ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_pbh_problemactivity',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_pbh_problemactivity ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_adjustment',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_adjustment ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_bpm_interface_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_prlt_bpm_interface_info ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_bpm_form_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bpm_form_info ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_om_order_item_his_vertical',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_item_his_vertical ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_inf_subs_drop_reason_ex',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_subs_drop_reason_ex ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_inf_legalization_cz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_legalization_cz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_pbh_problemprocess',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_pbh_problemprocess ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_apply_detail',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_apply_detail ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_om_order_line_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_line_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_inf_subscriber_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_subscriber_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_inf_offering_inst_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_offering_inst_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_t_pbh_problemworkitem',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_pbh_problemworkitem ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_credit_balance',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_credit_balance ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sr_contact_log',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsr_contact_log ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_om_order_ext_node_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_ext_node_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_sr_reception',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsr_reception ',
            conn_timeout = None,
            cmd_timeout = None, 
        )
#####################Job onice en autoys
        #TaskXGX = BashOperator(
        #    task_id='dwhods_stg_dwhods_ext_inf_prod_inst_his',##s(dwhods_ext_inf_prod_inst_his)
        #    #ssh_conn_id=ssh_conn_id,
        #    bash_command='date;hostname;sleep 60',
        #CAFksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_prod_inst_his ',
        #   conn_timeout = None,
        #   cmd_timeout = None, 
        #)

        Task21G6 = SSHOperator(
            task_id='dwhods_stg_dwhods_ext_ar_invoice',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_invoice ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

    # Creación grupo de tareas
    with TaskGroup("DWHODS_EXT_PreSharedSources") as group7:##El grupo7 depende de la finalizacion del task_id='inicia_malla_ods'

        Task1G7 = SSHOperator(
            task_id='dwhods_ext_om_order_item',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_item ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G7 = SSHOperator(
            task_id='dwhods_ext_acct_bill_cycle',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_acct_bill_cycle ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G7 = SSHOperator(
            task_id='dwhods_ext_inf_subscriber',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_subscriber ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G7 = SSHOperator(
            task_id='dwhods_ext_ar_invoice_detail',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnar_invoice_detail_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G7 = SSHOperator(
            task_id='dwhods_ext_inf_address',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_address ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G7 = SSHOperator(
            task_id='dwhods_ext_om_order_item_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_item_his_period ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G7 = SSHOperator(
            task_id='dwhods_ext_addr_reference',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_addr_reference ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G7 = SSHOperator(
            task_id='dwhods_ext_inf_offering_inst',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_offering_inst ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G7 = SSHOperator(
            task_id='dwhods_ext_debt_collection_detail',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndebt_collection_detail ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G7 = SSHOperator(
            task_id='dwhods_ext_om_order_item_vertical',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_item_vertical ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G7 = SSHOperator(
            task_id='dwhods_ext_bc_acct',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnbc_acct ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G7 = SSHOperator(
            task_id='dwhods_ext_inf_offering_prop',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_offering_prop ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G7 = SSHOperator(
            task_id='dwhods_ext_om_order_line',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_line ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G7 = SSHOperator(
            task_id='dwhods_ext_bc_customer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnbc_customer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G7 = SSHOperator(
            task_id='dwhods_ext_inf_party_certificate',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_party_certificate ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G7 = SSHOperator(
            task_id='dwhods_ext_pm_entity_attr',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnpm_entity_attr ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G7 = SSHOperator(
            task_id='dwhods_ext_bc_subscriber',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnbc_subscriber ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G7 = SSHOperator(
            task_id='dwhods_ext_om_operation_reason_his_cz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_operation_reason_his_cz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G7 = SSHOperator(
            task_id='dwhods_ext_remedy_bi_bitacora',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnremedy_bi_bitacora ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G7 = SSHOperator(
            task_id='dwhods_ext_davox_detalle_factura_cuenta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_detalle_factura_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G7 = SSHOperator(
            task_id='dwhods_ext_om_order',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G7 = SSHOperator(
            task_id='dwhods_ext_remedy_bi_cerrados_dia_anterior',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnremedy_bi_cerrados_dia_anterior ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G7 = SSHOperator(
            task_id='dwhods_ext_davox_impuesto_detalle_fac_cue',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndavox_impuesto_detalle_fac_cue ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G7 = SSHOperator(
            task_id='dwhods_ext_om_order_ext_node',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnom_order_ext_node ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G7 = SSHOperator(
            task_id='dwhods_ext_remedy_bi_pendientes_dia',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnremedy_bi_pendientes_dia ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G7 = SSHOperator(
            task_id='dwhods_ext_debt_collection',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqndebt_collection ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G7 = SSHOperator(
            task_id='dwhods_ext_sr_contact_reason',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsr_contact_reason ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G7 = SSHOperator(
            task_id='dwhods_ext_sys_employee_attr',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnsys_employee_attr ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G7 = SSHOperator(
            task_id='dwhods_ext_t_bpm_serviceim_cpe_accountid',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bpm_serviceim_cpe_accountid ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G7 = SSHOperator(
            task_id='dwhods_ext_t_bpm_serviceim_cpe_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bpm_serviceim_cpe_info ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G7 = SSHOperator(
            task_id='dwhods_ext_t_bpm_serviceim_cpeid',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_bpm_serviceim_cpeid ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G7 = SSHOperator(
            task_id='dwhods_ext_t_ss_questionanswer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqnt_ss_questionanswer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G7 = SSHOperator(
            task_id='dwhods_ext_inf_account',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_account ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G7 = SSHOperator(
            task_id='dwhods_ext_inf_bill_medium',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_bill_medium ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G7 = SSHOperator(
            task_id='dwhods_ext_inf_contact',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_contact ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G7 = SSHOperator(
            task_id='dwhods_ext_inf_customer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_customer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task37G7 = SSHOperator(
            task_id='dwhods_ext_inf_entity_attr_ex',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_entity_attr_ex ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task38G7 = SSHOperator(
            task_id='dwhods_ext_inf_individual',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhext_sqninf_individual ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        ##Configuración dependencias tareas grupo7
        #Task1G7 >> Task6G7 >> Task10G7 >> Task13G7 >> Task16G7 >> Task19G7 >> Task22G7 >> Task25G7 >> Task27G7 >> Task28G7 >> Task29G7 >> Task30G7 >> Task31G7 >> Task32G7 >> Task33G7 >> Task34G7 >> Task35G7 >> Task36G7 >> Task37G7 >> Task38G7
        #Task2G7 >> Task7G7 >> Task11G7 >> Task14G7 >> Task17G7 >> Task20G7 >> Task23G7 >> Task26G7
        #Task3G7 >> Task8G7 >> Task12G7 >> Task15G7 >> Task18G7 >> Task21G7 >> Task24G7
        #Task4G7 >> Task9G7
        #Task5G7

        #Configuración dependencias tareas grupo7 solicitado -- WO0000001973092 - Javier Martinez
        Task1G7 >> Task10G7 >> Task13G7 >> Task16G7 >> Task19G7 >> Task22G7 >> Task25G7 >> Task27G7 >> Task28G7 >> Task29G7 >> Task30G7 >> Task31G7 >> Task32G7 >> Task33G7 >> Task34G7 >> Task35G7 >> Task36G7 >> Task37G7 >> Task38G7
        Task2G7 >> Task7G7 >> Task11G7 >> Task14G7 >> Task17G7 >> Task20G7 >> Task23G7 >> Task26G7
        Task3G7 >> Task8G7 >> Task12G7 >> Task15G7 >> Task18G7 >> Task21G7 >> Task24G7
        Task4G7 >> Task9G7
        Task5G7 >> Task6G7

    # Creación grupo de tareas
    with TaskGroup("DWHODS_STG_PreSharedSources") as group8:##El grupo8 depende de la finalizacion de las tareas del grupo7--DWHODS_EXT_PreSharedSources & grupo6 DWHODS_STG_SharedSources 

        Task1G8 = SSHOperator(
            task_id='dwhods_stg_om_order_item_vertical',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_item_vertical ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G8 = SSHOperator(
            task_id='dwhods_stg_debt_collection',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndebt_collection ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G8 = SSHOperator(
            task_id='dwhods_stg_acct_bill_cycle',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_acct_bill_cycle ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G8 = SSHOperator(
            task_id='dwhods_stg_inf_subscriber',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_subscriber ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G8 = SSHOperator(
            task_id='dwhods_stg_ar_invoice_detail',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnar_invoice_detail ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G8 = SSHOperator(
            task_id='dwhods_stg_om_order_line',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_line ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G8 = SSHOperator(
            task_id='dwhods_stg_inf_account',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_account ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G8 = SSHOperator(
            task_id='dwhods_stg_addr_reference',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_addr_reference ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G8 = SSHOperator(
            task_id='dwhods_stg_om_operation_reason_his_cz',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_operation_reason_his_cz ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G8 = SSHOperator(
            task_id='dwhods_stg_pm_entity_attr',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnpm_entity_attr ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G8 = SSHOperator(
            task_id='dwhods_stg_inf_bill_medium',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_prlinf_bill_medium ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G8 = SSHOperator(
            task_id='dwhods_stg_bc_acct',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnbc_acct ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G8 = SSHOperator(
            task_id='dwhods_stg_om_order',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G8 = SSHOperator(
            task_id='dwhods_stg_remedy_bi_bitacora',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnremedy_bi_bitacora ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G8 = SSHOperator(
            task_id='dwhods_stg_inf_contact',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_contact ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G8 = SSHOperator(
            task_id='dwhods_stg_bc_customer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnbc_customer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G8 = SSHOperator(
            task_id='dwhods_stg_om_order_ext_node',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_ext_node ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G8 = SSHOperator(
            task_id='dwhods_stg_remedy_bi_cerrados_dia_anterior',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnremedy_bi_cerrados_dia_anterior ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G8 = SSHOperator(
            task_id='dwhods_stg_inf_customer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_customer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task20G8 = SSHOperator(
            task_id='dwhods_stg_bc_subscriber',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnbc_subscriber ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task21G8 = SSHOperator(
            task_id='dwhods_stg_om_order_item',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_item ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task22G8 = SSHOperator(
            task_id='dwhods_stg_remedy_bi_pendientes_dia',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnremedy_bi_pendientes_dia ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task23G8 = SSHOperator(
            task_id='dwhods_stg_inf_entity_attr_ex',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_entity_attr_ex ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task24G8 = SSHOperator(
            task_id='dwhods_stg_davox_detalle_factura_cuenta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_detalle_factura_cuenta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task25G8 = SSHOperator(
            task_id='dwhods_stg_om_order_item_his',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnom_order_item_his ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task26G8 = SSHOperator(
            task_id='dwhods_stg_sr_contact_reason',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsr_contact_reason ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task27G8 = SSHOperator(
            task_id='dwhods_stg_inf_individual',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_individual ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task28G8 = SSHOperator(
            task_id='dwhods_stg_davox_impuesto_detalle_fac_cue',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqndavox_impuesto_detalle_fac_cue ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task29G8 = SSHOperator(
            task_id='dwhods_stg_inf_offering_inst',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_offering_inst ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task30G8 = SSHOperator(
            task_id='dwhods_stg_sys_employee_attr',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnsys_employee_attr ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task31G8 = SSHOperator(
            task_id='dwhods_stg_inf_offering_prop',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_offering_prop ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task32G8 = SSHOperator(
            task_id='dwhods_stg_t_bpm_serviceim_cpe_accountid',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bpm_serviceim_cpe_accountid ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task33G8 = SSHOperator(
            task_id='dwhods_stg_inf_party_certificate',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqninf_party_certificate ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task34G8 = SSHOperator(
            task_id='dwhods_stg_t_bpm_serviceim_cpe_info',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bpm_serviceim_cpe_info ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task35G8 = SSHOperator(
            task_id='dwhods_stg_t_bpm_serviceim_cpeid',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_bpm_serviceim_cpeid ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task36G8 = SSHOperator(
            task_id='dwhods_stg_t_ss_questionanswer',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhstg_sqnt_ss_questionanswer ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        #Configuración dependencias tareas grupo8
        Task1G8 >> Task6G8 >> Task10G8 >> Task14G8 >> Task18G8 >> Task22G8 >> Task26G8 >> Task30G8 >> Task32G8 >> Task34G8 >> Task35G8 >> Task36G8
        Task2G8 >> Task7G8 >> Task11G8 >> Task15G8 >> Task19G8 >> Task23G8 >> Task27G8 >> Task31G8 >> Task33G8
        Task3G8 >> Task8G8 >> Task12G8 >> Task16G8 >> Task20G8 >> Task24G8 >> Task28G8
        Task4G8 >> Task9G8 >> Task13G8 >> Task17G8 >> Task21G8 >> Task25G8 >> Task29G8
        Task5G8

    # Creación grupo de tareas
    with TaskGroup("DWHODS_Datasets") as group9: ##El grupo9 depende de la finalizacion de las tareas de los  grupos 2-4-6-8.

        Task1G9 = SSHOperator(
            task_id='dwhsp_ds_clientes',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_clientes ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task2G9 = SSHOperator(
            task_id='dwhsp_ds_suscripciones',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_suscripciones ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task3G9 = SSHOperator(
            task_id='dwhsp_ds_cuentas',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_cuentas ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task4G9 = SSHOperator(
            task_id='dwhsp_ds_parque',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_parque ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task5G9 = SSHOperator(
            task_id='dwhsp_ds_reclamos',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_reclamos ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task6G9 = SSHOperator(
            task_id='dwhsp_ds_recaudo',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_recaudo ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task7G9 = SSHOperator(
            task_id='dwhsp_ds_peticiones',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_peticiones ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task8G9 = SSHOperator(
            task_id='dwhsp_ds_planta',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_planta ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task9G9 = SSHOperator(
            task_id='dwhsp_ds_contactos',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_contactos ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task10G9 = SSHOperator(
            task_id='dwhsp_ds_instalaciones',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_instalaciones ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task11G9 = SSHOperator(
            task_id='dwhsp_ds_facturacion',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_facturacion ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task12G9 = SSHOperator(
            task_id='dwhsp_ds_averias',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_averias ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task13G9 = SSHOperator(
            task_id='dwhsp_ds_altas',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_altas ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task14G9 = SSHOperator(
            task_id='dwhsp_ds_bajas',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_bajas ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task15G9 = SSHOperator(
            task_id='dwhsp_ds_migraciones',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_migraciones ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task16G9 = SSHOperator(
            task_id='dwhsp_ds_svas',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_svas ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task17G9 = SSHOperator(
            task_id='dwhsp_ds_ajustes',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_ajustes ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task18G9 = SSHOperator(
            task_id='dwhsp_ds_bandejas',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_bandejas ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        Task19G9 = SSHOperator(
            task_id='dwhsp_ds_cartera',
            ssh_conn_id=ssh_conn_id,
            command='date;hostname;sleep 60',
            ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion dwhsp_ds_cartera ',
            conn_timeout = None,
            cmd_timeout = None, 
        )

        #Configuración dependencias tareas grupo9
        Task1G9 >> Task3G9 >> [Task4G9,Task5G9,Task6G9,Task7G9]
        Task2G9 >> [Task4G9,Task5G9,Task6G9,Task7G9]
        Task4G9 >> Task8G9 >> Task11G9 >> Task17G9 >>Task19G9
        Task5G9 >> Task9G9 >> Task12G9 >> Task18G9
        Task6G9
        Task7G9 >> Task10G9 >> [Task13G9,Task14G9,Task15G9,Task16G9]
        
    adicion = SSHOperator(
        task_id='DWHODS_Adicionales', ## Esta tarea depende de la finalizacion de las tareas del grupo 9.
        ssh_conn_id=ssh_conn_id,
        command='date;hostname;sleep 60',
        ##command='ksh /archivos/Shells/DS_RunJob_Airflow.sh Produccion DWHODS_Adicionales ',
        conn_timeout = None,
        cmd_timeout = None, 
        )

    Fin = SSHOperator(
        task_id='fin_malla_ods', ## Esta tarea depende de la finalizacion de las tareas del grupo 9.
        ssh_conn_id=ssh_conn_id,
        command='date;hostname;sleep 60',
        ##command='ksh /archivos/Shells/mailfinmallaods.sh ',
        conn_timeout = None,
        cmd_timeout = None, 
        )

    # Configuración dependencias flujo del DAG
    Inicio >> [group1, group7]
    group2 >> group3
    group4 >> group5
    [group6,group7] >> group8
    [group2, group4,group6,group8] >> group9
    group9 >> [Fin, adicion]
    #Dependencias tareas Grupos 1-2
    Task1G1 >> Task1G2
    Task2G1 >> Task2G2
    Task3G1 >> Task3G2
    Task4G1 >> Task4G2
    Task5G1 >> Task5G2
    Task6G1 >> Task6G2
    Task7G1 >> Task7G2
    Task8G1 >> Task8G2
    Task9G1 >> Task9G2
    Task10G1 >> Task10G2
    Task11G1 >> Task11G2
    Task12G1 >> Task12G2
    Task13G1 >> Task13G2
    Task14G1 >> Task14G2
    Task15G1 >> Task15G2
    Task16G1 >> Task16G2
    Task17G1 >> Task17G2
    Task18G1 >> Task18G2
    Task19G1 >> Task19G2
    Task20G1 >> Task20G2
    Task21G1 >> Task21G2
    Task22G1 >> Task22G2
    Task23G1 >> Task23G2
    Task24G1 >> Task24G2
    Task25G1 >> Task25G2
    Task26G1 >> Task26G2
    Task27G1 >> Task27G2
    Task28G1 >> Task28G2
    Task29G1 >> Task29G2
    Task30G1 >> Task30G2
    Task31G1 >> Task31G2
    Task32G1 >> Task32G2
    Task33G1 >> Task33G2
    Task34G1 >> Task34G2
    Task35G1 >> Task35G2
    Task36G1 >> Task36G2
    Task37G1 >> Task37G2
    Task38G1 >> Task38G2
    #Dependencias Grupos 3-4
    Task1G3 >> Task20G4
    Task2G3 >> Task37G4
    Task3G3 >> Task3G4
    Task4G3 >> Task36G4
    Task5G3 >> Task2G4
    Task6G3 >> Task35G4
    Task7G3 >> Task1G4
    Task8G3 >> Task34G4
    Task9G3 >> Task19G4
    Task10G3 >> Task33G4
    Task11G3 >> Task18G4
    Task12G3 >> Task32G4
    Task13G3 >> Task17G4
    Task14G3 >> Task31G4
    Task15G3 >> Task16G4
    Task16G3 >> Task30G4
    Task17G3 >> Task15G4
    Task18G3 >> Task29G4
    Task19G3 >> Task14G4
    Task20G3 >> Task28G4
    Task21G3 >> Task13G4
    Task22G3 >> Task27G4
    Task23G3 >> Task12G4
    Task24G3 >> Task26G4
    Task25G3 >> Task11G4
    Task26G3 >> Task25G4
    Task27G3 >> Task10G4
    Task28G3 >> Task24G4
    Task29G3 >> Task9G4
    Task30G3 >> Task23G4
    Task31G3 >> Task8G4
    Task32G3 >> Task22G4
    Task33G3 >> Task7G4
    Task34G3 >> Task21G4
    Task35G3 >> Task6G4
    Task36G3 >> Task4G4
    Task37G3 >> Task5G4
    #Dependencias Grupos 5-6
    Task1G5 >> Task1G6
    Task2G5 >> Task2G6
    Task3G5 >> Task3G6
    Task4G5 >> Task4G6
    Task5G5 >> Task5G6
    Task6G5 >> Task6G6
    Task7G5 >> Task7G6
    Task8G5 >> Task8G6
    Task9G5 >> Task9G6
    Task10G5 >> Task10G6
    Task11G5 >> Task11G6
    Task12G5 >> Task12G6
    Task13G5 >> Task13G6
    Task14G5 >> Task14G6
    Task15G5 >> Task15G6
    Task16G5 >> Task16G6
    Task17G5 >> Task17G6
    Task18G5 >> Task18G6
    Task19G5 >> Task19G6
    Task20G5 >> Task20G6
    Task21G5 >> Task21G6
