import zmq
import threading
import json
import boto3
import ast
import os

selfLocation = 2 ###########CHANGE THIS
ownCalendar = []
partialLog = "" #is a string
matrixClock = [[0 for x in range(4)] for y in range(4)] 
try:
    os.remove("matrixClock.txt")
except OSError:
    pass
def sendToOregon(message):
    #pass
    contextZero = zmq.Context()
    socketZero = context.socket(zmq.REQ)
    socketZero.connect("tcp://35.166.40.6:5555") #uncomment with the Ip of the AMI to which you are SENDING
    socketZero.send_json(message)

def sendToOhio(message):
    pass
    contextOne = zmq.Context()
    socketOne = context.socket(zmq.REQ)
    socketOne.connect("tcp://18.188.14.182:5555") #uncomment with the Ip of the AMI to which you are SENDING
    socketOne.send_json(message)

def sendToNV(message):
    pass
    ''' contextTwo = zmq.Context()
    socketTwo = context.socket(zmq.REQ)
    socketTwo.connect("tcp://34.230.43.14:5555") #uncomment with the Ip of the AMI to which you are SENDING
    socketTwo.send_json(message) '''

def sendToNC(message):
    #pass
    contextThree = zmq.Context()
    socketThree = context.socket(zmq.REQ)
    socketThree.connect("tcp://54.153.22.117:5555") #uncomment with the Ip of the AMI to which you are SENDING
    socketThree.send_json(message)

def infiniteLoop1():
    global matrixClock
    global selfLocation
    global ownCalendar
    global partialLog
    while True:
        message = json.loads(socket.recv_json()).split(':')        
        messageLocation = int(message[1])
        messageClock = json.loads(message[2])
        incomingPartialLog = message[3]
        for k in range(0,4):
            matrixClock[selfLocation][k] = max(matrixClock[selfLocation][k], messageClock[messageLocation][k])
        for k in range(0,4):
            for l in range(0,4):
                matrixClock[k][l] = max(matrixClock[k][l], messageClock[k][l])
        matrixClock[selfLocation][selfLocation] = matrixClock[selfLocation][selfLocation] + 1 
        matrixClockFile = open("matrixClock.txt", 'a')
        matrixClockFile.writelines("\n"+str(matrixClock))
        matrixClockFile.close()
        a = incomingPartialLog.split(" ") #you are making the assumption that this is add
        b = a[2:7]
        event = [ast.literal_eval(i) for i in b[:-1]] + [ast.literal_eval(b[-1])]
        if(a[0]=='add'): #add
            ownCalendar.append(event) #add the event to your own calendar own replicated log
        else:
            ownCalendar.remove(event)
        print(ownCalendar)
        ownCalendarFile = open("ownCalendar.txt", 'w')
        ownCalendarFile.writelines("\n"+str(ownCalendar))
        ownCalendarFile.close()
        socket.send(b"World") #i guess you have to do this

def infiniteLoop2():#this is 0. ohio is 1. nv is 2 
    global matrixClock
    global selfLocation
    global ownCalendar
    global partialLog
    print("Enter 0 for add event and 1 for delete event")
    print("Enter numeric names for gods sake")
    print("Day goes from 0 to 6")
    print("startTime goes from 0 to 47 corresponding to each half hours of the day")
    print("endTime goes from 0 to 47 corresponding to each half hours of the day")
    print("Participants go from 0 to 3. Your node should always be here. Put them in array separated by commas. example [1,2,3]")
    print("Enter name day startTime endTime participants")
    print("Your node is " + str(selfLocation))
    while True:
        matrixClock[selfLocation][selfLocation] = matrixClock[selfLocation][selfLocation] + 1
        matrixClockFile = open("matrixClock.txt", 'a')
        matrixClockFile.writelines("\n"+str(matrixClock))
        matrixClockFile.close()
        x = input()
        a = x.split(" ")
        event = [ast.literal_eval(i) for i in a[:-1]] + [ast.literal_eval(a[-1])]
        y = str(selfLocation) + ":"
        if(event[0]==0): #we are in add event phase
            for i in ownCalendar:  #add/del, name,day,start_time, end_time, participantsArray
                if( (i[1]==event[2]) and ( (i[2]<=event[4]) and (i[3] >= event[3])) and (not set(i[4]).isdisjoint(event[5])) ): #no overlaps?
                    print("Cannot schedule this event since there is overlap")
                    break;
            else:
                ownCalendar.append(event[1:])#add the event to your own calendar own replicated log
                ownCalendarFile = open("ownCalendar.txt", 'w')
                ownCalendarFile.writelines("\n"+str(ownCalendar))
                ownCalendarFile.close()
                print(ownCalendar)
                partialLog = "add " + x[1:] + " " + str(selfLocation) ##########WORK ON THIS
                partialLogFile = open("partialLog.txt", 'w')
                partialLogFile.writelines("\n"+str(partialLog))
                partialLogFile.close()
                print(event[5])
                for dudes in event[5]:
                    if (dudes == 0):
                        sendToOregon(json.dumps("0:"+y+json.dumps(matrixClock)+":"+partialLog))
                    elif (dudes == 1):
                        sendToOhio(json.dumps("0:"+y+json.dumps(matrixClock)+":"+partialLog))
                    elif (dudes == 2):
                        sendToNV(json.dumps("0:"+y+json.dumps(matrixClock)+":"+partialLog))  
                    elif (dudes == 3):
                        sendToNC(json.dumps("0:"+y+json.dumps(matrixClock)+":"+partialLog)) #need to change this at the other side
        else: # we are in delete phase
            ownCalendar.remove(event[1:])
            ownCalendarFile = open("ownCalendar.txt", 'w')
            ownCalendarFile.writelines("\n"+str(ownCalendar))
            ownCalendarFile.close()
            print(ownCalendar)
            partialLog = "del " + x[1:] + " " + str(selfLocation)##########WORK ON THIS
            partialLogFile = open("partialLog.txt", 'w')
            partialLogFile.writelines("\n"+str(partialLog))
            partialLogFile.close()
            print(event[5])
            for dudes in event[5]:
                if (dudes == 0):
                    sendToOregon(json.dumps("1:"+y+json.dumps(matrixClock)+":"+partialLog))
                elif (dudes == 1):
                    sendToOhio(json.dumps("1:"+y+json.dumps(matrixClock)+":"+partialLog))
                elif (dudes == 2):
                    sendToNV(json.dumps("1:"+y+json.dumps(matrixClock)+":"+partialLog))  
                elif (dudes == 3):
                    sendToNC(json.dumps("1:"+y+json.dumps(matrixClock)+":"+partialLog)) #need to change this at the other side

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")
thread1 = threading.Thread(target=infiniteLoop1)
thread1.start()
thread2 = threading.Thread(target=infiniteLoop2)
thread2.start()