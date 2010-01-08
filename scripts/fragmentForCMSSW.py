
import FWCore.ParameterSet.Config as cms
from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper

def customise_for_gc(process):
	try:
		maxevents = __MAX_EVENTS__
		process.maxEvents = cms.untracked.PSet(
			input = cms.untracked.int32(maxevents)
		)
	except:
		pass

	try:
		tmp = __SKIP_EVENTS__
		process.source = cms.Source("PoolSource",
			skipEvents = cms.untracked.uint32(__SKIP_EVENTS__),
			fileNames = cms.untracked.vstring(__FILE_NAMES__)
		)
	except:
		pass

	try:
		secondary = __FILE_NAMES2__
		process.source.secondaryFileNames = cms.untracked.vstring(secondary)
	except:
		pass

	try:
		lumirange = __LUMI_RANGE__
		process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange(lumirange)
	except:
		pass

	if hasattr(process, "RandomNumberGeneratorService"):
		randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService)
		randSvc.populate()

	return (process)

process = customise_for_gc(process)

