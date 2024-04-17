import requests
import json
import sys
import datetime
import csv

def main():
    url_base = 'https://api.gov.prismacloud.io'
    access_key_id = ''
    access_key_secret = ''

    # Get a short lived access token
    token = get_token(url_base, access_key_id, access_key_secret)
    
    # Get the list of accounts
    accounts = get_accounts(url_base, token)

    # Get the credit usage over the past 3 months for each account
    credits = get_usage_count_past_N_months(url_base, token, 3)

    # Join the account group data with the credit usage data
    credits_with_accounts = join_credit_usage_with_accounts(credits, accounts)

    # Write the output to a CSV file
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d_%H%M%S")
    outfile = 'PrismaCloudCreditUsage_' + formatted_time + '.csv'
    write_output_file(credits_with_accounts, outfile)
    print("Wrote credit data to %s" % (outfile))


# Returns string containing the short lived auth token
def get_token(url_base, access_key_id, access_key_secret):
    url_endpoint = url_base + '/login'
    payload = '{"username": "' + access_key_id + '", "password": "' + access_key_secret + '"}'
    headers = {
    'Content-Type': 'application/json; charset=UTF-8',
    'Accept': 'application/json; charset=UTF-8'
    }

    response = requests.request("POST", url_endpoint, headers=headers, data=payload)

    # Parse the JSON string
    data = json.loads(response.text)
    token = ''
    # Check if 'token' key exists
    if 'token' in data:
        # Extract the token value
        token = data['token']
    else:
        # If 'token' key does not exist, exit with error code
        print("Error: 'token' key not found in JSON data.")
        sys.exit(1)
    return token


# Returns json array containing cloud accounts as described 
# in the output here: https://pan.dev/prisma-cloud/api/cspm/get-cloud-accounts/#responses
def get_accounts(url_base, token):
    url_endpoint = url_base + '/cloud'
    payload={}
    headers = {
    'Accept': 'application/json; charset=UTF-8',
    'x-redlock-auth': token
    }

    response = requests.request("GET", url_endpoint, headers=headers, data=payload)
    data = json.loads(response.text)
    return data


# Returns json array containing usage data as described
# here: https://pan.dev/prisma-cloud/api/cspm/license-usage-count-by-cloud-paginated-v-2/
# Note: the output of this function follows 'next_page' and appends everything to a 
# single json output array.
def get_usage_count_past_N_months(url_base, token, n_months=3):
    url_endpoint = url_base + '/license/api/v2/usage'
    payload = '{"accountIds":[],"cloudTypes":["aws","azure","oci","alibaba_cloud","gcp","others"],"accountGroupIds":[],"timeRange":{"type":"relative","value":{"amount":"' + str(n_months) + '","unit":"month"}},"limit":10}'
    headers = {
    'Content-Type': 'application/json;charset=UTF-8',
    'Accept': 'application/json;charset=UTF-8',
    'x-redlock-auth': token
    }

    response = requests.request("POST", url_endpoint, headers=headers, data=payload)
    data = json.loads(response.text)
    account_stats = data['items']
    nextPageToken = data['nextPageToken']

    while nextPageToken != '' and nextPageToken is not None:
        payload = '{"accountIds":[],"cloudTypes":["aws","azure","oci","alibaba_cloud","gcp","others"],"accountGroupIds":[],"timeRange":{"type":"relative","value":{"amount":"3","unit":"month"}},"limit":10, "pageToken":"' + nextPageToken + '"}'
        response = requests.request("POST", url_endpoint, headers=headers, data=payload)
        data = json.loads(response.text)
        items = data['items']
        account_stats = account_stats + items
        nextPageToken = data['nextPageToken']

    return account_stats


def join_credit_usage_with_accounts(credit_usage_count, accounts):
    # map each accountId to its list of account groups so we can do quick lookups by accountId
    acct_id_to_acct_group_dict = {}
    for a in accounts:
        if (a['accountId'] in acct_id_to_acct_group_dict or
            a['accountId'] == '' or
            a['accountId'] is None or
            a['groups'] == '' or
            a['groups'] is None):
            print("Error: duplicate account ID in join_credit_usage_with_accounts.")
            sys.exit(1)
        acct_id_to_acct_group_dict[a['accountId']] = a['groups']
    print(acct_id_to_acct_group_dict)
    
    joined_data = []
    for c in credit_usage_count:
        acct_id = c['account']['id']
        if acct_id is None or acct_id == "" :
            print("Error: join_credit_usage_with_accounts: acct_id is not present.")
        if acct_id in acct_id_to_acct_group_dict:
            groups = acct_id_to_acct_group_dict[acct_id]
            for g in groups:
                rec = {}
                rec['accountName'] = c['account']['name']
                rec['accountId'] = acct_id
                rec['groupId'] = g['id']
                rec['groupName'] = g['name']
                rec['cloudType'] = c['cloudType']
                rec['total'] = c['total']
                rec['container'] = c['resourceTypeCount']['container']
                rec['iam'] = c['resourceTypeCount']['iam']
                rec['container_caas'] = c['resourceTypeCount']['container_caas']
                rec['data_store'] = c['resourceTypeCount']['data_store']
                rec['agentless_host'] = c['resourceTypeCount']['agentless_host']
                rec['host'] = c['resourceTypeCount']['host']
                rec['serverless'] = c['resourceTypeCount']['serverless']
                rec['iaas'] = c['resourceTypeCount']['iaas']
                rec['waas'] = c['resourceTypeCount']['waas']
                rec['agentless_container'] = c['resourceTypeCount']['agentless_container']
                
                joined_data.append(rec)
    print(joined_data)
    return joined_data
            
def write_output_file(credits_with_accounts, outfile):
    fieldnames = ['accountName', 'accountId', 'groupName', 'groupId', 'cloudType',
                  'total', 'container', 'iam', 'container_caas', 'data_store',
                  'agentless_host', 'host', 'serverless', 'iaas',
                  'waas', 'agentless_container']
    with open(outfile, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for c in credits_with_accounts:
            writer.writerow(c)


if __name__ == '__main__':
    main()