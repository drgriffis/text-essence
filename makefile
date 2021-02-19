SHELL=/bin/bash
PY=python
DATADIR=CORD-19

download_CORD19_corpus:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	DISTRIBDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} DistribDirectory); \
	if [ ! -d "$${DISTRIB}" ]; then \
		echo "Distribution directory $${DISTRIBDIR} does not exist.  Attempt to recursively create it? [y/n]"; \
		read CREATEDISTRIBDIR; \
		if [ "$${CREATEDISTRIBDIR}" = "y" ]; then \
			mkdir -p $${DISTRIBDIR}; \
		else \
			exit; \
		fi; \
	fi; \
	cd $${DISTRIBDIR}; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/cord-19_${CORPUS}.tar.gz; \
	mv cord-19_${CORPUS}.tar.gz cord-19_${CORPUS}.tar.bz2; \
	tar -xzvf cord-19_${CORPUS}.tar.bz2; \
	mv ${CORPUS}/* .; \
	rmdir ${CORPUS}

download_CORD19_corpus_OLD:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	DISTRIBDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} DistribDirectory); \
	if [ ! -d "$${EXTRACTDIR}" ]; then \
		echo "Extraction directory $${EXTRACTDIR} does not exist.  Attempt to recursively create it? [y/n]"; \
		read CREATEDISTRIBDIR; \
		if [ "$${CREATEDISTRIBDIR}" = "y" ]; then \
			mkdir -p $${DISTRIBDIR}; \
		else \
			exit; \
		fi; \
	fi; \
	cd $${DISTRIBDIR}; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/${CORPUS}/comm_use_subset.tar.gz; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/${CORPUS}/noncomm_use_subset.tar.gz; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/${CORPUS}/custom_license.tar.gz; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/${CORPUS}/biorxiv_medrxiv.tar.gz; \
	wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/${CORPUS}/metadata.csv

extract_corpus:
	@if [ -z "${CORPUS}" ]; then \
		echo "Must specify CORPUS"; \
		exit; \
	fi; \
	DISTRIBDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} DistribDirectory); \
	EXTRACTDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	FORMAT=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} Format); \
	ABSTRACTONLY=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} AbstractOnly); \
	if [ "$${ABSTRACTONLY}" = "True" ]; then \
		ABSTRACTONLYFLAG="--abstract-only"; \
	else \
		ABSTRACTONLYFLAG=; \
	fi; \
	if [ -z "${REFCORPUS}" ]; then \
		REFDIRFLAG=; \
	else \
		REFDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${REFCORPUS} DistribDirectory); \
		REFDIRFLAG="--reference-directory $${REFDIR}"; \
	fi; \
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
		--format $${FORMAT} \
		$${ABSTRACTONLYFLAG} \
		$${REFDIRFLAG} \
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
	INPUTDIR=$${CORPUSDIR}/tokenized${SPEC}; \
	if [ -z "${OUTPUTDIR}" ]; then \
		OUTPUTDIR=$${INPUTDIR}; \
	else \
		OUTPUTDIR=${OUTPUTDIR}; \
	fi; \
	cd $${JET}; \
	${PY} -m preprocessing.tagcorpus \
		-i $${INPUTDIR}/preprocessed_corpus.txt \
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



distribute_JET:
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
	if [ -z "${OUTPUT}" ]; then \
		echo "Must specify OUTPUT"; \
		exit; \
	fi; \
	CORPUSDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${CORPUS} ExtractedDirectory); \
	TERMINOLOGYDIR=$$(${PY} -m cli_configparser.read_setting -c config.ini ${TERMINOLOGY} ExtractedDirectory); \
	OUTPUTDIR=$${CORPUSDIR}/tokenized${SPEC}; \
	if [ ! -d "$${OUTPUTDIR}/embeddings" ]; then \
		echo "No embeddings found in $${OUTPUTDIR}"; \
		exit; \
	fi; \
	cd $${OUTPUTDIR}/embeddings${EMBSPEC}; \
	if [ -d "__distrib__" ]; then \
		rm -rf __distrib__; \
	fi; \
	mkdir __distrib__; \
	${PY} -m pyemblib.convert --from word2vec-binary --to word2vec-text \
		words.bin __distrib__/words.txt; \
	${PY} -m pyemblib.convert --from word2vec-binary --to word2vec-text \
		terms.bin __distrib__/terms.txt; \
	${PY} -m pyemblib.convert --from word2vec-binary --to word2vec-text \
		entities.bin __distrib__/concepts.txt; \
	cp $${TERMINOLOGYDIR}/tokenized${SPEC}/terminology.term_to_string_map.txt \
		__distrib__/term_ID_to_string_map.txt; \
	echo "--------------------------" > __distrib__/README; \
	echo "Pre-trained JET embeddings" >> __distrib__/README; \
	echo "--------------------------" >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	echo "The contents of this zip file are as follows:" >> __distrib__/README; \
	echo "  README :: description and JET training configuration" >> __distrib__/README; \
	echo "  concepts.txt :: Embeddings for each SNOMED-CT code in the 2020-03-09 release" >> __distrib__/README; \
	echo "  terms.txt :: Embeddings for each unique term (description string) in the 2020-03-09 release of SNOMED-CT" >> __distrib__/README; \
	echo "  words.txt :: Static word embeddings" >> __distrib__/README; \
	echo "  term_ID_to_string_map.txt :: Mapping from term embedding IDs (in terms.txt) to SNOMED-CT strings" >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	echo "Embedding files are in the standard format readable by gensim or the lightweight pyemblib Python modules." >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	echo "Example loading with gensim:" >> __distrib__/README; \
	echo "  from gensim.models import KeyedVectors" >> __distrib__/README; \
	echo "  embeddings = KeyedVectors.load_word2vec_format('concepts.txt', binary=False)" >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	echo "Example loading with pyemblib:" >> __distrib__/README; \
	echo "  import pyemblib" >> __distrib__/README; \
	echo "  embeddings = pyemblib.read('concepts.txt', mode=pyemblib.Mode.Text)" >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	echo "" >> __distrib__/README; \
	head -n 14 config.log >> __distrib__/README; \
	cd __distrib__; \
	zip ${OUTPUT}.zip *.txt README; \

run_dashboard:
	@export FLASK_APP=nearest_neighbors/dashboard/app.py; \
	${PY} -m flask run

load_definitions:
	@${PY} -m nearest_neighbors.utils.load_definitions \
		-d ../data/SNOMEDCT_US__DEFs__2017AB.csv \
		-c config.ini \
		-l ../data/load_definitions.log

visualization:
	@${PY} -m nearest_neighbors.calculation.prepare_visualization \
		-i "../data/Full Embedding Sets" \
		-b "../data/SNOMEDCT_US__groups__2019AB.csv" \
		-o "nearest_neighbors/dashboard/static/visualization.json" \
		-f "Chemicals & Drugs,Disorders,Procedures,Physiology,Anatomy,Activities & Behaviors,Devices,Genes & Molecular Sequences,Phenomena,Occupations" \
		-c config.ini \
		-l ../data/prepare_visualization.log
