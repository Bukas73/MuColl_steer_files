#!/usr/bin/python2 -E

import os
from os import walk
import sys
from optparse import OptionParser
import subprocess
import tarfile
import glob
import fileinput
import getpass
import string
import random

import htcondor
import classad

parser = OptionParser()
parser.add_option("-p","--UserProxy", dest="UserProxy", default=("/tmp/x509up_u%s"%(os.getuid())), help="Provide path to user proxy file")
parser.add_option("-x","--Executable", dest="Executable", help="path to Executable")
parser.add_option("-e","--Experiment", dest="Experiment", help="Name of the experiment (lux, lz, etc.)")
parser.add_option("","--UserEnv", dest="UserEnv", help="Environment variables need to be sandboxed")
parser.add_option("","--InputFileList", dest="InputFileList", help="Path to File containing list of input files")
parser.add_option("","--InputDir", dest="InputDir", help="Input directory to look for input files")
parser.add_option("-i","--UserInput", dest="UserInput", help="User Input files to ship with the job")
parser.add_option("-o","--OutputDir", dest="OutputDir", help="An OutputDir will be created in /hdfs/<experiment>/user/<username>/")
parser.add_option("-f","--HDFSProdDir", dest="HDFSProdDir", help="Directory where Executable places files for copying to HDFS. Can be None.")
parser.add_option("-u","--MakeUniqueOutput", action="store_true", dest="MakeUniqueOutput", help="Add unique tag to each output file stored in HDFS")
parser.add_option("-r","--Requirements", dest="Requirements", help="Additional job requirements")
parser.add_option("-m","--MemoryRequirements", dest="MemoryRequirements", help="Memory requirements (megabytes), default 2048")
parser.add_option("-d","--DiskRequirements", dest="DiskRequirements", type="int", help="Disk requirements (megabytes), default 2000")
parser.add_option("-a","--ExtraAttributes", dest="Attributes", help="Additional job attributes")
parser.add_option("-c","--ConfigFile", dest="ConfigFile", help="Config file to run")
parser.add_option("", "--nFilesPerJob", dest="nFilesPerJob", default=1, type="int", help="Number of input files per job")
parser.add_option("-w","--WorkFlow", dest="WorkFlow", help="WorkFlow name")
parser.add_option("", "--nJobs", dest="nJobs", default=1, type="int", help="Number of jobs with same configuration")
parser.add_option("", "--Arguments", dest="Arguments", help="Arguments for the script. format : <str1> <str2>")
parser.add_option("", "--TransferInputFiles", dest="TransferInputFiles", help="TranferInputFiles. format : </path/to/file>,</path/to/file>")


(opt,args) = parser.parse_args()
class CException(Exception):
    pass

class farmoutGeneric():
    def __init__(self):
        self.parentDir = os.path.dirname(os.path.realpath(__file__))
        self.UserProxy = opt.UserProxy
        self.Experiment = opt.Experiment
        self.UserEnv = opt.UserEnv if opt.UserEnv else ""
        self.SiteReq = 'TARGET.Arch == "X86_64" && (TARGET.HAS_OSG_WN_CLIENT=?=true || TARGET.IS_GLIDEIN=?=true)'
        self.MemReq = 2048
        self.DiskReq = 2000
        self.Attributes = ""
        self.usercode="user_code.tgz"
        self.InputDir = opt.InputDir if opt.InputDir else ""
        self.InputFileList = opt.InputFileList if opt.InputFileList else ""
        self.nFilesPerJob = opt.nFilesPerJob
        self.nJobs = opt.nJobs
        self.WorkFlow = opt.WorkFlow
        self.Schedd = htcondor.Schedd()
        self.UserJobs = []
        self.DAGFile = ""
        self.Arguments = opt.Arguments if opt.Arguments else ""
        self.UserInput = opt.UserInput if opt.UserInput else ""
        self.TransferInputFiles = opt.TransferInputFiles if opt.TransferInputFiles else ""
        self.SubmitDir = ""
        self.MyRandomNumber=12345
        self.MyInputFile=""
        
        if not opt.WorkFlow :
            parser.print_help()
            raise CException("[-w] <str> or --WorkFlow=<str> is required")

        if opt.Executable :
            self.Executable = opt.Executable 
            self.UserEnv += " RUN_PROGRAM=./%s " % os.path.basename(opt.Executable)
            if self.TransferInputFiles :
                self.TransferInputFiles += ","
            self.TransferInputFiles += opt.Executable
        else :
            parser.print_help()
            raise CException("Name of the executable is required.")

        if opt.Requirements :
            self.SiteReq += " && %s" % opt.Requirements

        if opt.MemoryRequirements :
            self.MemReq = opt.MemoryRequirements

        if opt.DiskRequirements :
            self.DiskReq = opt.DiskRequirements

        if opt.Attributes :
            self.Attributes = opt.Attributes

        if opt.OutputDir :
            self.OutputDir=opt.OutputDir
            self.UserEnv+=" MyOutputDir=%s "%self.OutputDir

        if opt.HDFSProdDir:
            self.HDFSProdDir=opt.HDFSProdDir
            self.UserEnv+=" OUTPUT_DIR=%s "%self.HDFSProdDir
        else :
          parser.print_help()
          raise CException("HDFSProdDir is required. Set it to None if your executable copies its output to HDFS.")

        if opt.MakeUniqueOutput:
          self.UserEnv += " MakeUniqueOutput=1 "

        if self.Experiment :
          self.UserEnv += " MyExperiment=%s " % self.Experiment
        else :
          parser.print_help()
          raise CException("Name of the experiment (lux, lz, etc.) is required.")
        
    def getfilename(self, a) :
        return str(os.path.splitext(os.path.basename(a))[0])

    def job_description(self, f=None, irng=None) :

        if f is not None :
            ## Strip off
            f = map(lambda s: s.strip(), f)
            newf = ', '.join(f)  ### 'a.txt, b.txt, c.txt'
            if "/hdfs/" in newf or "root://" in newf:
                self.MyInputFile = "'%s' " % (newf.replace(', ',' '))
            else :
                f = map(lambda s: os.path.abspath(s), f)
                newf = ', '.join(f)  ### 'a.txt, b.txt, c.txt'
                #self.TransferInputFiles = newf
                newf = map(lambda s: os.path.basename(s), f)
                newf = ', '.join(newf)
                self.Arguments += newf.replace(', ',' ')
                self.MyInputFile = "'%s' " % self.Arguments
                
            fname = self.getfilename(f[0])
            if irng is not None :
                fname = fname+"_"+str(irng)

        else :
            if irng is not None :
                fname = str(irng)
            else :
                fname="0"
                

        jobDir=self.SubmitDir+"/"+fname
        if not os.path.exists(jobDir) :
            os.makedirs(jobDir)

        jobEnv  = " MyRandomNumber=%i MyInputFile=%s " %(self.MyRandomNumber, self.MyInputFile)
        jobEnv += self.UserEnv
        jobEnv = '"%s"' % jobEnv
        joutput = jobDir+"/"+fname+".out"
        jerror  = jobDir+"/"+fname+".err"
        jlog    = jobDir+"/"+fname+".log"
        jobfile = jobDir+"/"+fname+".job"

        joutput = os.path.abspath(joutput)
        jerror = os.path.abspath(jerror)
        jlog = os.path.abspath(jlog)

	sys.argv[0]

	pathname = os.path.dirname(sys.argv[0])        
	farmouthome = os.path.abspath(pathname)
	executable = farmouthome + "/runJobAndCopy.sh"

        
        jdlfile = (
            'X509UserProxy           = %s \n\
Universe                 = vanilla \n\
Executable               = %s \n\
GetEnv                   = True \n\
+RequiresCVMFS           = True \n\
Environment              = %s \n\
Copy_To_Spool            = false \n\
Notification             = never \n\
WhenToTransferOutput     = On_Exit_Or_Evict \n\
ShouldTransferFiles      = yes \n\
on_exit_remove           = True \n\
+IsFastQueueJob          = True \n\
request_memory           = %s \n\
request_disk             = %d \n\
Requirements             = %s \n\
+JobFlavour              = "tomorrow" \n\
%s \n\
periodic_hold            = DiskUsage/1024 > 2000*10 \n\
job_ad_information_attrs = MachineAttrGLIDEIN_Site0,MachineAttrName0 \n\
Arguments                = %s \n\
Transfer_Input_Files     = %s \n\
output                   = %s \n\
error                    = %s \n\
Log                      = %s \n\
Queue \n'  % (self.UserProxy, executable, jobEnv, 
              self.MemReq, self.DiskReq * 1024, self.SiteReq, self.Attributes, self.Arguments, self.TransferInputFiles,
              joutput, jerror, jlog
              )
            )

        with open(jobfile,'w') as sbf :
            sbf.write(jdlfile)

        self.UserJobs.append(jobfile)
        return self.UserJobs


    def createJDL(self):
        if self.InputFileList != "" :
            if self.InputDir != "" or self.nJobs >1 :
                raise CException("InputFileList, InputDir and nJobs can not be defined together")
            else :
                try :
                    ifl = [l.strip('\n') for l in open(self.InputFileList,'r')]
                    RandomList=random.sample(range(1,100 * len(ifl)), len(ifl))
                    if self.nFilesPerJob > 1 :
                        chunk = [ifl[i:i+self.nFilesPerJob] for i in range(0, len(ifl), self.nFilesPerJob)]
                        for ifileList in chunk :
                            istr=("%i-%i" % (ifl.index(ifileList[0]), ifl.index(ifileList[-1])))
                            ujobs = self.job_description(ifileList, str(istr))
                    else :
                        for ifile in ifl:
                            self.MyRandomNumber = RandomList[ifl.index(ifile)]
                            ifileList=[]
                            ifileList.append(ifile)
                            ujobs = self.job_description(ifileList)

                except IOError :
                    raise

        elif self.InputDir != "" :
            if self.nJobs > 1 :
                raise CException("InputDir and nJobs can not be defined together")
            try :
                #(_,_,ifl) = walk(self.InputDir).next()
                ifl = [os.path.join(self.InputDir,f) for f in os.listdir(self.InputDir)]
                RandomList=random.sample(range(1,100 * len(ifl)), len(ifl))
                if self.nFilesPerJob > 1 :
                    chunk = [ifl[i:i+self.nFilesPerJob] for i in range(0, len(ifl), self.nFilesPerJob)]
                    for ifileList in chunk :
                        istr=("%i-%i" % (ifl.index(ifileList[0]), ifl.index(ifileList[-1])))
                        self.MyRandomNumber = RandomList[ifl.index(ifileList[0])]
                        ujobs = self.job_description(ifileList, str(istr))
                else :
                    for ifile in ifl:
                        self.MyRandomNumber = RandomList[ifl.index(ifile)]
                        ifileList=[]
                        ifileList.append(ifile)
                        ujobs = self.job_description(ifileList)

            except IOError :
                raise

        elif self.nJobs > 1 :
            if self.Executable is None :
                raise CException("Executable can not be None. --Executable=</path/to/file>")
            
            try :
                RandomList = random.sample(range(1,100 * self.nJobs), self.nJobs)
                for n in range(0, self.nJobs):
                    self.MyRandomNumber = RandomList[n]
                    ujobs = self.job_description(None, n)
                    
            except IOError :
                raise
        else :
            try :
                if self.Arguments == "" :
                    print ">>> WARNING : You don't have any arguments for your Executable %s " % self.Executable
                    print "Setting up a random string as argument"
                    self.Arguments = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(6))
                ujobs = self.job_description()
            except OSError :
                raise
                
        print "%i Jobs are created in %s" % (len(ujobs), self.SubmitDir)

    ## Check the user grid proxy info
    def check_proxy(self):
        try:
            with open(self.UserProxy) as pf:
                cmd=("voms-proxy-info --timeleft --file=%s"%self.UserProxy)
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()[0]
                if int(p) < (3600*24*4):
                    raise CException("Proxy is valid for less than 5 days. Run voms-proxy-init --valid=142:00")
        except IOError :
            raise CException("No Proxy File. Run voms-proxy-init")

    
    # Prepare the sandbox and tar all the usercode
    def gatherUserInput(self):
        try:
            if self.UserInput != "" :
                tar = tarfile.open(self.SubmitDir+"/"+self.usercode,"w:gz")
                f = self.UserInput.split()

            for name in f:
                tar.add(f)
            tar.close();
        except IOError :
            print "Could not prepare usercode with user inputs"
            raise
        finally :
            self.InputFiles += self.SubmitDir+"/"+self.usercode
            

    def runUserConfig(self):
        try:
            if self.ConfigFile != "" :
                self.UserInput.append(self.ConfigFile)
        except IOError :
            print "Cant read ConfigFile %s " % self.ConfigFile
            raise

    def createInputFiles(self):
        try :
            if self.UserInput :
                l = [j for j in self.UserInput.split()]
                for i in l :
                    if os.path.isfile(i) :
                        i = os.path.abspath(i)
                        self.InputFiles += i+" "
                    else :
                        self.InputFiles += i+" "
        except OSError :
            raise
            
            

    def submitJobs(self):
        try :
            for job in self.UserJobs :
                cmd=("condor_submit %s" %job)
                p = subprocess.Popen(cmd, shell=True, 
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.PIPE)
                out,err = p.communicate()
                print out, err
                                     
        except OSError : 
            raise

    def createDAG(self):
        pass
                
    def createSubmitDir(self):
        self.SubmitDir = "/nfs_scratch/"+getpass.getuser()+"/"+self.WorkFlow
        if not os.path.exists(self.SubmitDir) :
            os.makedirs(self.SubmitDir)
        else :
            raise CException("SubmitDir %s exits. Remove/Rename it" % self.SubmitDir)

def main():
    '''
    FarmoutGeneric Script
    '''
    farmG = farmoutGeneric()
    farmG.check_proxy()
    farmG.createSubmitDir()
    farmG.createInputFiles()
    farmG.createJDL()
    farmG.submitJobs()
            

if __name__ == "__main__" :
    try :
        ret = main()
    except KeyboardInterrupt:
        ret = None

    sys.exit(ret)

    
    
