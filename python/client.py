import comm_pb2
import socket               
import time
import struct




def buildSigninJob(username, password,ownerId):
    
    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()    

    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB
    
    r.body.job_op.data.name_space = "sign_in"
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED
    
    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "operation"
    r.body.job_op.data.options.value = "sign_in"
    
    email = comm_pb2.NameValueSet()
    email.node_type = comm_pb2.NameValueSet.NODE
    email.name = "email"
    email.value = username
    
    psw = comm_pb2.NameValueSet()
    psw.node_type = comm_pb2.NameValueSet.NODE
    psw.name = "password"
    psw.value = password
    
    r.body.job_op.data.options.node.extend([email, psw])
    
    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1
    
    msg = r.SerializeToString()
    return msg

def buildCourseDescJob(courseName, ownerId):
    
    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()    

    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB
    
    r.body.job_op.data.name_space = "getdescription"
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED
    
    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.VALUE
    r.body.job_op.data.options.name = "coursename"
    r.body.job_op.data.options.value = courseName
    
    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1
    
    msg = r.SerializeToString()
    return msg

def buildNS():
    r = comm_pb2.Request()

    r.body.space_op.action = comm_pb2.NameSpaceOperation.ADDSPACE

    
    r.header.originator = 100
    r.header.tag = str(int(round(time.time() * 1000)))
    r.header.routing_id = comm_pb2.Header.NAMESPACES

    m = r.SerializeToString()
    return m
    
def buildListCourse(ownerId):

    jobId = str(int(round(time.time() * 1000)))
    
    r = comm_pb2.Request()
    
    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB
    
    r.body.job_op.data.name_space = "listcourses"
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED
    
    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "operation"
    r.body.job_op.data.options.value = "listcourses"
    
    uId = comm_pb2.NameValueSet()
    uId.node_type = comm_pb2.NameValueSet.NODE
    uId.name = "uId"
    uId.value = "12"
      
    r.body.job_op.data.options.node.extend([uId])
    
    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1
    
    msg = r.SerializeToString()
    return msg

def buildIntercluster(ownerId):

    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()

    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB

    r.body.job_op.data.name_space = "intercluster"
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED

    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "operation"
    r.body.job_op.data.options.value = "interclsuter"

    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1

    msg = r.SerializeToString()
    return msg


def buildListRegisteredCourse(user):

    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()

    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB

    r.body.job_op.data.name_space = "listregisteredcourses"
    r.body.job_op.data.owner_id = 1234
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED

    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "operation"
    r.body.job_op.data.options.value = "List Register Courses"

    uId = comm_pb2.NameValueSet()
    uId.node_type = comm_pb2.NameValueSet.NODE
    uId.name = "userName"
    uId.value = user

    r.body.job_op.data.options.node.extend([uId])

    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1

    msg = r.SerializeToString()
    return msg

def buildRegisterJob(courseName, ownerId , username):
    jobId = str(int(round(time.time() * 1000)))

    r = comm_pb2.Request()

    r.body.job_op.job_id = jobId
    r.body.job_op.action = comm_pb2.JobOperation.ADDJOB

    r.body.job_op.data.name_space = "register_course"
    r.body.job_op.data.owner_id = ownerId
    r.body.job_op.data.job_id = jobId
    r.body.job_op.data.status = comm_pb2.JobDesc.JOBQUEUED

    r.body.job_op.data.options.node_type = comm_pb2.NameValueSet.NODE
    r.body.job_op.data.options.name = "Operation"
    r.body.job_op.data.options.value = "Course Registration"

    course = comm_pb2.NameValueSet()
    course.node_type = comm_pb2.NameValueSet.NODE
    course.name = "Course Name"
    course.value = courseName

    user = comm_pb2.NameValueSet()
    user.node_type = comm_pb2.NameValueSet.NODE
    user.name = "User"
    user.value = username

    r.body.job_op.data.options.node.extend([course, user])

    r.header.originator = 100
    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.toNode = 1

    msg = r.SerializeToString()
    return msg


def sendMsg(msg_out, port, host):
    s = socket.socket()         
#    host = socket.gethostname()
#    host = "192.168.0.87"

    s.connect((host, port))        
    msg_len = struct.pack('>L', len(msg_out))    
    s.sendall(msg_len + msg_out)
    len_buf = receiveMsg(s, 4)
    msg_in_len = struct.unpack('>L', len_buf)[0]
    msg_in = receiveMsg(s, msg_in_len)
    
    r = comm_pb2.Request()
    r.ParseFromString(msg_in)
#    print msg_in
#    print r.body.job_status 
#    print r.header.reply_msg
#    print r.body.job_op.data.options

    s.close
    return r
def receiveMsg(socket, n):
    buf = ''
    while n > 0:        
        data = socket.recv(n)                  
        if data == '':
            raise RuntimeError('data not received!')
        buf += data
        n -= len(data)
    return buf  


def getBroadcastMsg(port):
    # listen for the broadcast from the leader"
          
    sock = socket.socket(socket.AF_INET,  # Internet
                        socket.SOCK_DGRAM)  # UDP

    sock.bind(('', port))
   
    data = sock.recv(1024)  # buffer size is 1024 bytes
    return data





if __name__ == '__main__':
    #host="10.0.0.5"
    #port="5574"

    host="localhost"
    port="5574"

    port = int(port)
    whoAmI = 1;
    print "----------------------------"
    print "Welcome to our MOOC client !\n"
    login = False
    print("Signin to the system:")
    username = raw_input("Enter Username:")
    password = raw_input("Enter Password:")

    while True:
        input = raw_input("\nPlease select your desirable action:\n0. Quit\n1. Inter-Cluster\nEnter your choice:")
        print "---------------------------------------------------"
        if input == "1":
            interclusterJob = buildIntercluster(whoAmI)
            result = sendMsg(interclusterJob, port, host)
            print "Course\t\tDescription"
            print "---------------------------------------------------"
            print "\n"
            datalength = len(result.body.job_status.data)
            for y in range(0,datalength):
                print(result.body.job_status.data[y].options.name)
                print "\n"
                length = len(result.body.job_status.data[y].options.node)
                for x in range(0,length):
                    print result.body.job_status.data[y].options.node[x].name + "\t\t" + result.body.job_status.data[y].options.node[x].value
                print "\n"

        if input == "0":
            print("Thanks for using our MOOC! See you soon ...\n")
            break

        print "---------------------------------------------------"
