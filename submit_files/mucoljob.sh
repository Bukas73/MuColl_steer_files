#!/bin/bash

mkdir jobHome

mv ilcsoft.sh jobHome 
mv sim_steer_1.py jobHome
mv reco_steer_1.xml jobHome
mv hs_m20_d1.hepmc jobHome
mv lctuple_steer_v2.xml jobHome
mv Pandora* jobHome

WORKDIR=`pwd`/jobHome


#singularity run -B /cvmfs --contain --home=$WORKDIR --workdir=$WORKDIR /cvmfs/unpacked.cern.ch/registry.hub.docker.com/infnpd/mucoll-ilc-framework:1.6-centos8

#/bin/bash ilcsoft.sh

#singularity exec  -B /cvmfs --contain --home=$WORKDIR --workdir=$WORKDIR /cvmfs/unpacked.cern.ch/registry.hub.docker.com/infnpd/mucoll-ilc-framework:1.6-centos8 /bin/bash ilcsoft.sh $*

singularity exec -B /cvmfs --contain --home=$WORKDIR --workdir=$WORKDIR /cvmfs/cms.hep.wisc.edu/mucol/reference/mucoll_1.6_v02-07MC.sif /bin/bash ilcsoft.sh $*

mv jobHome/*.out .
mv jobHome/*.root .
mv jobHome/*.slcio .
### clean dir

rm -rf jobHome

