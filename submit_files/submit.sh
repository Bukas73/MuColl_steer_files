
for param in ['m0_d0'] #,'m+1_d0','m+2_d0','m-1_d0','m-2_d0','m0_d+1','m0_d+2','m0_d-1','m0_d-2']
do
	python runWiscJobs.py \
		--WorkFlow hs_m20_d1_5_test \
		--Executable=/nfs_scratch/lucarpen/MuonCollider/Submit/mucoljob.sh \
		--Arguments=10 \
		--nJobs=1 \
		--TransferInputFiles=ilcsoft.sh,sim_steer_1.py,reco_steer_1.xml,lctuple_steer_v2.xml,hs_scalar_files/hs_m20_d1.hepmc,PandoraSettings/PandoraLikelihoodData12EBin.xml,PandoraSettings/PandoraLikelihoodData9EBin.xml,PandoraSettings/PandoraSettingsDefault.xml,MuonCVXDDigitiser.h,MyG4UniversalFluctuationForSi.h \
		--HDFSProdDir None \
		--Experiment mucol \
		--DiskRequirements=4000 \
		--MemoryRequirements=8000 \

done
