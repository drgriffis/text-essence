SHELL=/bin/bash
PY=python

extract_corpus:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	DISTRIBDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} DistribDirectory); \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	if [ ! -d "$${EXTRACTDIR}" ]; then \
		echo "Extraction directory $${EXTRACTDIR} does not exist.  Attempt to recursively create it? [y/n]"; \
		read CREATEEXTRACTDIR; \
		if [ "$${CREATEEXTRACTDIR}" = "y" ]; then \
			mkdir -p $${EXTRACTDIR}; \
		else \
			exit; \
		fi; \
	fi; \
	${PY} -m corpus.extract_corpus \
		-d $${DISTRIBDIR} \
		-o $${EXTRACTDIR}/corpus.txt \
		-l $${EXTRACTDIR}/corpus.log

preprocess:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	SPEC=; \
	if [ -z "${LOWER}" ]; then \
		LOWERFLAG=; \
	else \
		LOWERFLAG="--lower"; \
		SPEC="$${SPEC}_lower"; \
	fi; \
	if [ -z "${NOPUNCT}" ]; then \
		PUNCTFLAG=; \
	else \
		PUNCTFLAG="--strip-punctuation"; \
		SPEC="$${SPEC}_nopunct"; \
	fi; \
	if [ -z "${NORMDIGITS}" ]; then \
		NORMDIGITSFLAG=; \
	else \
		NORMDIGITSFLAG="--normalize-digits"; \
		SPEC="$${SPEC}_normdigits"; \
	fi; \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	OUTPUTDIR=$${EXTRACTDIR}/tokenized$${SPEC}; \
	if [ ! -d "$${OUTPUTDIR}" ]; then \
		mkdir $${OUTPUTDIR}; \
	fi; \
	${PY} -m corpus.preprocess_corpus \
		-i $${EXTRACTDIR}/corpus.txt \
		-o $${OUTPUTDIR}/preprocessed_corpus.txt \
		$${LOWERFLAG} \
		$${PUNCTFLAG} \
		$${NORMDIGITSFLAG} \
		-l $${OUTPUTDIR}/preprocessed_corpus.log

extract_terminology:
	@if [ -z "${TERMINOLOGY}" ]; then \
		echo "TERMINOLOGY must be specified"; \
		echo "(corresponds to a section in config.ini)"; \
		exit; \
	fi; \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting config.ini ${TERMINOLOGY} ExtractedDirectory); \
	${PY} -m terminology.snomed_ct.extract_terminology \
		--concepts $$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} ConceptsFile) \
		--descriptions $$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} DescriptionsFile) \
		--definitions $$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} DefinitionsFile) \
		-o $${EXTRACTDIR}/snomedct_terminology.txt \
		-l $${EXTRACTDIR}/snomedct_terminology.log

normalize_terminology:
	@if [ -z "${TERMINOLOGY}" ]; then \
		echo "TERMINOLOGY must be specified"; \
		echo "(corresponds to a section in config.ini)"; \
		exit; \
	fi; \
	SPEC=; \
	if [ -z "${LOWER}" ]; then \
		LOWERFLAG=; \
	else \
		LOWERFLAG="--lower"; \
		SPEC="$${SPEC}_lower"; \
	fi; \
	if [ -z "${NOPUNCT}" ]; then \
		PUNCTFLAG=; \
	else \
		PUNCTFLAG="--strip-punctuation"; \
		SPEC="$${SPEC}_nopunct"; \
	fi; \
	if [ -z "${NORMDIGITS}" ]; then \
		NORMDIGITSFLAG=; \
	else \
		NORMDIGITSFLAG="--normalize-digits"; \
		SPEC="$${SPEC}_normdigits"; \
	fi; \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting config.ini ${TERMINOLOGY} ExtractedDirectory); \
	${PY} -m terminology.preprocess_terminology \
		-i $${EXTRACTDIR}/snomed_terminology.txt \
		-o $${EXTRACTDIR}/tokenized$${SPEC}/terminology.txt \
		$${LOWERFLAG} \
		$${PUNCTFLAG} \
		$${NORMDIGITSFLAG} \
		-l $${EXTRACTDIR}/tokenized$${SPEC}/terminology.log

compile_terminology:
	@if [ -z "${TERMINOLOGY}" ]; then \
		echo "TERMINOLOGY must be specified"; \
		echo "(corresponds to a section in config.ini)"; \
		exit; \
	fi; \
	if [ -z "${SPEC}" ]; then \
		echo "SPEC must be specified"; \
		exit; \
	fi; \
	JET=$$(${PY} -m cli_configparser.read_setting -c config.ini General JETInstallation); \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} ExtractedDirectory); \
	cd $${JET}; \
	${PY} -m preprocessing.compile_terminology \
		-i $${EXTRACTDIR}/tokenized$${SPEC}/terminology.txt \
		-o $${EXTRACTDIR}/tokenized$${SPEC} \
		--tokenizer PreTokenized \
		-l $${EXTRACTDIR}/tokenized$${SPEC}/terminology_compilation.log

tag_corpus:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	if [ -z "${TERMINOLOGY}" ]; then \
		echo "Must specify TERMINOLOGY"; \
		exit; \
	fi; \
	if [ -z "${SPEC}" ]; then \
		echo "Must specify SPEC"; \
		exit; \
	fi; \
	if [ -z "${THREADS}" ]; then \
		THREADS=8; \
	else \
		THREADS=${THREADS}; \
	fi; \
	JET=$$(${PY} -m cli_configparser.read_setting -c config.ini General JETInstallation); \
	CORPUSDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	TERMINOLOGYDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} ExtractedDirectory); \
	OUTPUTDIR=$${CORPUSDIR}/tokenized${SPEC}; \
	cd $${JET}; \
	${PY} -m preprocessing.tagcorpus \
		-i $${OUTPUTDIR}/preprocessed_corpus.txt \
		-t $${TERMINOLOGYDIR}/tokenized${SPEC}/terminology.ngram_term_map.pkl.gz \
		-o $${OUTPUTDIR}/preprocessed_corpus.$${TERMINOLOGY}__tokenized${SPEC}.annotations \
		-l $${OUTPUTDIR}/preprocessed_corpus.$${TERMINOLOGY}__tokenized${SPEC}.annotations.log \
		--threads $${THREADS} \
		--max-lines 100 \
		--tokenizer PreTokenized

run_JET:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	if [ -z "${TERMINOLOGY}" ]; then \
		echo "Must specify TERMINOLOGY"; \
		exit; \
	fi; \
	if [ -z "${SPEC}" ]; then \
		echo "Must specify SPEC"; \
		exit; \
	fi; \
	RANDOMTAG=$$(head -c 500 /dev/urandom | tr -dc 'a-zA-Z' | fold -w 25 | head -n 1); \
	JET=$$(${PY} -m cli_configparser.read_setting -c config.ini General JETInstallation); \
	CORPUSDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	TERMINOLOGYDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} ExtractedDirectory); \
	OUTPUTDIR=$${CORPUSDIR}/tokenized${SPEC}; \
	if [ ! -d "$${OUTPUTDIR}/embeddings" ]; then \
		mkdir $${OUTPUTDIR}/embeddings; \
	fi; \
	cd $${JET}; \
	make bin/JET NAME=$${RANDOMTAG}; \
	bin/JET$${RANDOMTAG} \
		-iters 10 \
		-threads 8 \
		-size 300 \
		-binary 1 \
		-model $${OUTPUTDIR}/embeddings \
		-plaintext $${OUTPUTDIR}/preprocessed_corpus.txt \
		-annotations $${OUTPUTDIR}/preprocessed_corpus.$${TERMINOLOGY}__tokenized${SPEC}.annotations \
		-word-vocab $${OUTPUTDIR}/vocabs.word \
		-term-vocab $${OUTPUTDIR}/vocabs.term \
		-term-map $${TERMINOLOGYDIR}/tokenized${SPEC}/terminology.term_to_entity_map.txt \
		-string-map $${TERMINOLOGYDIR}/tokenized${SPEC}/terminology.term_to_string_map.txt
