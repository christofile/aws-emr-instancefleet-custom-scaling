import json
import boto3
from datetime import datetime,timedelta

client = boto3.client('cloudwatch')
emr = boto3.client('emr')
dynamodb = boto3.client('dynamodb')

def taskscaleininstancefleet(event,clusterid,taskminunit,scaleunit,endtime,scaleincooldown):
    response = emr.list_instance_fleets(
        ClusterId=clusterid);
        
    fleets = {}
    fleets["InstanceFleets"]={};
    
    cnresponse = emr.describe_cluster(ClusterId=clusterid)
    clustername=cnresponse['Cluster']['Name']
    
    for f in response["InstanceFleets"]:
        if f["InstanceFleetType"] == "TASK":
            fleetid=f["Id"];
            targetspot=f["TargetSpotCapacity"];
            targetondemand=f["TargetOnDemandCapacity"];
            provisionedspot=f["ProvisionedSpotCapacity"];
            
            #在缩容前，先判断是否表中是否有记录，没有则创建，有的话判断此次时间是否已满足CoolDownPeriod
            dbresponse=dynamodb.scan(TableName='instancefleetscaling',FilterExpression="clusterid= :n",ExpressionAttributeValues={":n":{"S":clusterid}})
            print(dbresponse)
            if dbresponse['Items']:
                lastscalingtime=dbresponse['Items'][0]['timestamp']['S'];
            else:
                dynamodb.put_item(TableName='instancefleetscaling', Item={'clusterid':{'S':clusterid},'clustername':{'S':clustername},'timestamp':{'S':endtime},'coretimestamp':{'S':endtime}})
                break
            newscalingtime=(datetime.strptime(lastscalingtime, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=scaleincooldown)).isoformat(timespec='seconds')
            thisscalingtime=event['time'];
            
            print(lastscalingtime)
            print(newscalingtime)
            print(thisscalingtime)
            print(newscalingtime<thisscalingtime)
            if(newscalingtime<thisscalingtime):
                if taskminunit<=(targetspot-scaleunit):
                    newtargetspot=targetspot-scaleunit;
                else:
                    newtargetspot=taskminunit;
                #通过emr modify_instance_fleet修改TargetSpotCapacity减少scaleunit
                response = emr.modify_instance_fleet(
                        ClusterId=clusterid,
                        InstanceFleet={
                            'InstanceFleetId': fleetid,
                            'TargetOnDemandCapacity': targetondemand,
                            'TargetSpotCapacity': newtargetspot
                            
                        }
                    );
                #当执行了缩容，将执行的时间写入dynamodb
                dynamodb.update_item(TableName='instancefleetscaling', Key={'clusterid':{'S':clusterid},'clustername':{'S':clustername}}, UpdateExpression='SET #n = :n',ExpressionAttributeNames={'#n':'timestamp'},ExpressionAttributeValues={":n":{"S":endtime}})
                print(clusterid,'task scale in is done')
                
def corescaleininstancefleet(event,clusterid,coreminunit,corescaleunit,endtime,scaleincooldown):
    response = emr.list_instance_fleets(
        ClusterId=clusterid);
        
    fleets = {}
    fleets["InstanceFleets"]={};
    
    cnresponse = emr.describe_cluster(ClusterId=clusterid)
    clustername=cnresponse['Cluster']['Name']
    
    for f in response["InstanceFleets"]:
        if f["InstanceFleetType"] == "CORE":
            fleetid=f["Id"];
            targetspot=f["TargetSpotCapacity"];
            targetondemand=f["TargetOnDemandCapacity"];
            provisionedspot=f["ProvisionedSpotCapacity"];
            
            #在扩容前，先判断是否表中是否有记录，没有则创建，有的话判断此次时间是否已满足CoolDownPeriod
            dbresponse=dynamodb.scan(TableName='instancefleetscaling',FilterExpression="clusterid= :n",ExpressionAttributeValues={":n":{"S":clusterid}})
            print(dbresponse)
            if dbresponse['Items']:
                lastscalingtime=dbresponse['Items'][0]['coretimestamp']['S'];
            else:
                dynamodb.put_item(TableName='instancefleetscaling', Item={'clusterid':{'S':clusterid},'clustername':{'S':clustername},'timestamp':{'S':endtime},'coretimestamp':{'S':endtime}})
                break
            newscalingtime=(datetime.strptime(lastscalingtime, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=scaleincooldown)).isoformat(timespec='seconds')
            thisscalingtime=event['time'];
            
            print(lastscalingtime)
            print(newscalingtime)
            print(thisscalingtime)
            print(newscalingtime<thisscalingtime)
            if(newscalingtime<thisscalingtime):
                if coreminunit<=(targetspot-corescaleunit):
                    newtargetspot=targetspot-corescaleunit;
                else:
                    newtargetspot=coreminunit;
                #通过emr modify_instance_fleet修改TargetSpotCapacity减少scaleunit
                response = emr.modify_instance_fleet(
                        ClusterId=clusterid,
                        InstanceFleet={
                            'InstanceFleetId': fleetid,
                            'TargetOnDemandCapacity': targetondemand,
                            'TargetSpotCapacity': newtargetspot
                            
                        }
                    );
                #当执行了缩容，将执行的时间写入dynamodb
                dynamodb.update_item(TableName='instancefleetscaling', Key={'clusterid':{'S':clusterid},'clustername':{'S':clustername}}, UpdateExpression='SET #n = :n',ExpressionAttributeNames={'#n':'coretimestamp'},ExpressionAttributeValues={":n":{"S":endtime}})
                print(clusterid,'core scale in is done')

def taskscaleoutinstancefleet(event,clusterid,taskmaxunit,scaleunit,endtime,scaleoutcooldown):
    response = emr.list_instance_fleets(
        ClusterId=clusterid);
        
    fleets = {}
    fleets["InstanceFleets"]={};
    
    cnresponse = emr.describe_cluster(ClusterId=clusterid)
    clustername=cnresponse['Cluster']['Name']
    
    for f in response["InstanceFleets"]:
        if f["InstanceFleetType"] == "TASK":
            fleetid=f["Id"];
            targetspot=f["TargetSpotCapacity"];
            targetondemand=f["TargetOnDemandCapacity"];
            provisionedspot=f["ProvisionedSpotCapacity"];
            
            #在扩容前，先判断是否表中是否有记录，没有则创建，有的话判断此次时间是否已满足CoolDownPeriod
            dbresponse=dynamodb.scan(TableName='instancefleetscaling',FilterExpression="clusterid= :n",ExpressionAttributeValues={":n":{"S":clusterid}})
            if dbresponse['Items']:
                lastscalingtime=dbresponse['Items'][0]['timestamp']['S'];
            else:
                dynamodb.put_item(TableName='instancefleetscaling', Item={'clusterid':{'S':clusterid},'clustername':{'S':clustername},'timestamp':{'S':endtime},'coretimestamp':{'S':endtime}})
                break
            newscalingtime=(datetime.strptime(lastscalingtime, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=scaleoutcooldown)).isoformat(timespec='seconds')
            thisscalingtime=event['time'];
            
            print(lastscalingtime)
            print(newscalingtime)
            print(thisscalingtime)
            print(newscalingtime<thisscalingtime) 
            if(newscalingtime<thisscalingtime):
                if taskmaxunit>=targetspot+scaleunit:
                    newtargetspot=targetspot+scaleunit;
                else:
                    newtargetspot=taskmaxunit;
                #通过emr modify_instance_fleet修改TargetSpotCapacity增加scaleunit
                response = emr.modify_instance_fleet(
                        ClusterId=clusterid,
                        InstanceFleet={
                            'InstanceFleetId': fleetid,
                            'TargetOnDemandCapacity': targetondemand,
                            'TargetSpotCapacity': newtargetspot
                            
                        }
                    );
                #当执行了扩容，将执行的时间写入dynamodb
                dynamodb.update_item(TableName='instancefleetscaling', Key={'clusterid':{'S':clusterid},'clustername':{'S':clustername}}, UpdateExpression='SET #n = :n',ExpressionAttributeNames={'#n':'timestamp'},ExpressionAttributeValues={":n":{"S":endtime}})
                print(clusterid,'task scale out is done')
                
def corescaleoutinstancefleet(event,clusterid,coremaxunit,corescaleunit,endtime,scaleoutcooldown):
    response = emr.list_instance_fleets(
        ClusterId=clusterid);
        
    fleets = {}
    fleets["InstanceFleets"]={};
    
    cnresponse = emr.describe_cluster(ClusterId=clusterid)
    clustername=cnresponse['Cluster']['Name']
    
    for f in response["InstanceFleets"]:
        if f["InstanceFleetType"] == "CORE":
            fleetid=f["Id"];
            targetspot=f["TargetSpotCapacity"];
            targetondemand=f["TargetOnDemandCapacity"];
            provisionedspot=f["ProvisionedSpotCapacity"];
            
            #在扩容前，先判断是否表中是否有记录，没有则创建，有的话判断此次时间是否已满足CoolDownPeriod
            dbresponse=dynamodb.scan(TableName='instancefleetscaling',FilterExpression="clusterid= :n",ExpressionAttributeValues={":n":{"S":clusterid}})
            print(dbresponse)
            if dbresponse['Items']:
                lastscalingtime=dbresponse['Items'][0]['coretimestamp']['S'];
            else:
                dynamodb.put_item(TableName='instancefleetscaling', Item={'clusterid':{'S':clusterid},'clustername':{'S':clustername},'timestamp':{'S':endtime},'coretimestamp':{'S':endtime}})
                break
            newscalingtime=(datetime.strptime(lastscalingtime, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=scaleoutcooldown)).isoformat(timespec='seconds')
            thisscalingtime=event['time'];
            
            print(lastscalingtime)
            print(newscalingtime)
            print(thisscalingtime)
            print(newscalingtime<thisscalingtime) 
            if(newscalingtime<thisscalingtime):
                if coremaxunit>=targetspot+corescaleunit:
                    newtargetspot=targetspot+corescaleunit;
                else:
                    newtargetspot=coremaxunit;
                #通过emr modify_instance_fleet修改TargetSpotCapacity增加scaleunit
                response = emr.modify_instance_fleet(
                        ClusterId=clusterid,
                        InstanceFleet={
                            'InstanceFleetId': fleetid,
                            'TargetOnDemandCapacity': targetondemand,
                            'TargetSpotCapacity': newtargetspot
                            
                        }
                    );
                #当执行了扩容，将执行的时间写入dynamodb
                dynamodb.update_item(TableName='instancefleetscaling', Key={'clusterid':{'S':clusterid},'clustername':{'S':clustername}}, UpdateExpression='SET #n = :n',ExpressionAttributeNames={'#n':'coretimestamp'},ExpressionAttributeValues={":n":{"S":endtime}})
                print(clusterid,'core scale out is done')

def lambda_handler(event, context):
    # TODO implement
    
    emrresponse=emr.list_clusters(ClusterStates=['WAITING'])
    dynamodbresponse=dynamodb.scan(TableName='emrscaling')
    
    cn=[]
    i=0
    j=0
    cid=[]
    
    while(i < dynamodbresponse['Count']):
        cn.append(dynamodbresponse['Items'][i]['clustername']['S'])
        for j in cn:
            for c in emrresponse["Clusters"]:
                if c['Name']==j:
                    cid.append(c['Id']);
        i=i+1;
    
    print(cid);
    
    for i in cid:
        #print(dynamodbresponse)
        cnresponse = emr.describe_cluster(ClusterId=i)
        clustername=cnresponse['Cluster']['Name']
        
        dbresponse=dynamodb.scan(TableName='emrscaling',FilterExpression="clustername= :n",ExpressionAttributeValues={":n":{"S":clustername}})
        sisu=int(dbresponse['Items'][0]['taskscaleinspotunit']['N']);#TASK ScalingIn调整的Spot Unit
        sosu=int(dbresponse['Items'][0]['taskscaleoutspotunit']['N']);#TASK ScalingOut调整的Spot Unit
        
        coresisu=int(dbresponse['Items'][0]['corescaleinspotunit']['N']);#CORE ScalingIn调整的Spot Unit
        coresosu=int(dbresponse['Items'][0]['corescaleoutspotunit']['N']);#CORE ScalingOut调整的Spot Unit
        
        interval=int(dbresponse['Items'][0]['interval']['N']);#CloudWatch YARNMemoryAvailablePercentage Period值，单位为秒

        sicd=int(dbresponse['Items'][0]['scaleincooldownperiod']['N']);#ScaleInCoolDownPeriod，缩容，单位为秒
        socd=int(dbresponse['Items'][0]['scaleoutcooldownperiod']['N']);#ScaleOutCoolDownPeriod，扩容，单位为秒
        
        min=int(dbresponse['Items'][0]['taskminunit']['N']);#TASK Min Unit
        max=int(dbresponse['Items'][0]['taskmaxunit']['N']);#TASK Max Unit
        coremin=int(dbresponse['Items'][0]['coreminunit']['N']);#CORE Min Unit
        coremax=int(dbresponse['Items'][0]['coremaxunit']['N']);#CORE Max Unit
        
        HighYarnAvailMemory=float(dbresponse['Items'][0]['highyarnavailmemory']['N']);
        LowYarnAvailMemory=float(dbresponse['Items'][0]['lowyarnavailmemory']['N']);
        HighHDFSUtilization=float(dbresponse['Items'][0]['highhdfsutilization']['N']);
        LowHDFSUtilization=float(dbresponse['Items'][0]['lowhdfsutilization']['N']);
        
        #print(su,sicd,socd,min,max,HighYarnAvailMemory,LowYarnAvailMemory)
        
        et=event['time'];
        result = datetime.strptime(et, "%Y-%m-%dT%H:%M:%SZ") - timedelta(seconds=interval)
        st=result.isoformat(timespec='seconds').replace('+00:00', 'Z') #CloudWatch StartTime的时间设置为Endtime-Period
        
        #Yarn CloudWatch参数读取    
        response = client.get_metric_statistics(
        Namespace='AWS/ElasticMapReduce',
        MetricName='YARNMemoryAvailablePercentage',
        Dimensions=[
            {
                'Name': 'JobFlowId',
                'Value': i
            },
        ],
        StartTime=st,
        EndTime=et,
        Period=interval,
        Statistics=['Maximum']
        )
        print(response)
        
        #EMR InstanceFleet 参数调整
        for f in response['Datapoints']:
            YarnAvailMem=f['Maximum']
            if YarnAvailMem>=HighYarnAvailMemory:
                taskscaleininstancefleet(event,i,min,sisu,et,sicd)
            elif YarnAvailMem<=LowYarnAvailMemory:
                taskscaleoutinstancefleet(event,i,max,sosu,et,socd)
        
        #HDFS CloudWatch参数读取    
        hdfsresponse = client.get_metric_statistics(
        Namespace='AWS/ElasticMapReduce',
        MetricName='HDFSUtilization',
        Dimensions=[
            {
                'Name': 'JobFlowId',
                'Value': i
            },
        ],
        StartTime=st,
        EndTime=et,
        Period=interval,
        Statistics=['Maximum']
        )
        print(hdfsresponse)
        
        #EMR InstanceFleet 参数调整
        for f in hdfsresponse['Datapoints']:
            HDFSUtilization=f['Maximum']
            print(HDFSUtilization)
            if HDFSUtilization>=HighHDFSUtilization:
                print(YarnAvailMem)
                corescaleininstancefleet(event,i,coremin,coresisu,et,sicd)
            elif HDFSUtilization<=LowHDFSUtilization:
                corescaleoutinstancefleet(event,i,coremax,coresosu,et,socd)