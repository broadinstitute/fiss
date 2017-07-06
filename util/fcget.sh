#! /bin/bash

Version=0.0.1
BillingProject=nci-mnoble-bi-org
CancerProject=TCGA
Attribute=${1:-CNV__snp6}
Date=2016_11_03

echo "`basename $0` version $Version"
echo "Retrieving $Attribute from all sample sets in analyses__$Date spaces ..."

Spaces=`fissfc space_list --project $BillingProject | \
        awk '{print $NF}' | grep analyses`

find_file()
{
    fissfc attr_get --workspace $space \
                    --project $BillingProject \
                    --attributes $Attribute \
                    --entity $1 | \
                    cut -f2 | tail -1
}

for s in $Spaces ; do
    cohort=`echo $s | awk -F"__" '{print $NF}'`
    space=analyses__${Date}__$cohort
    cohort=`echo $cohort | tr [:lower:] [:upper:]`
    case $cohort in
        SKCM) sset=${cohort}-TM ;;
        LAML) sset=${cohort}-TB ;;
           *) sset=${cohort}-TP ;;
    esac
    sset=${CancerProject}-$sset
    echo
    echo $space
    echo $sset
    file_in_cloud=`find_file $sset`

    if [ -z "$file_in_cloud" ] ; then
        echo "No attribute $Attribute found for $sset, skipping ..."
        continue
    fi
    file_on_prem=`basename $file_in_cloud`
    does_filename_contain_sset=`echo $file_on_prem | grep $sset`
    # If
    if [ -z "$does_filename_contain_sset" ] ; then
        file_on_prem=${sset}.$file_on_prem
    fi
    \rm -f $file_on_prem
    gsutil cp $file_in_cloud $file_on_prem
done
