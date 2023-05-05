#!/bin/bash

WORKDIR=`pwd`

source /opt/ilcsoft/muonc/init_ilcsoft.sh

#git clone https://github.com/dally96/MuonColSim.git

#cd MuonColSim

#unzip NewData2.hepmc.zip

#cd MuonCutil/SoftCheck

GEO="/opt/ilcsoft/muonc/detector-simulation/geometries/MuColl_v1/MuColl_v1.xml"

ddsim --compactFile ${GEO} --inputFile hs_m20_d1.hepmc --steeringFile sim_steer_1.py &> $WORKDIR/sim.out 

Marlin --InitDD4hep_mod4.DD4hepXMLFile=${GEO} reco_steer_1.xml &> $WORKDIR/reco.out

Marlin lctuple_steer_v2.xml &> $WORKDIR/ntuple.out

#mv ntuple_tracker.root $WORKDIR/histograms.root 

mv lctuple_tracker.root $WORKDIR/LLP_m20_d1_5.root

#mv muonGun_sim_MuColl_v1.slcio $WORKDIR/SimHits.slcio 

