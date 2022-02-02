from types import SimpleNamespace
import csv

class BaseUMLSParser:
    def __init__(self, filepath):
        self._filepath = filepath
        self._stream = None
        self._reader = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self):
        self.close()

    def __iter__(self):
        if not self._stream:
            raise Exception('Must call parser open() method or use "with" context before iterating')
        return self

    def __next__(self):
        record = next(self._reader)
        return self.parseLine(record)

    def open(self):
        if self._stream:
            self.close()
        self._stream = open(self._filepath, 'r')
        self._reader = csv.reader(self._stream, delimiter='|')

    def close(self):
        if self._stream:
            self._stream.close()
            self._reader = None

class SemGroupsParser(BaseUMLSParser):
    def parseLine(self, record):
        (
            GROUPCODE,
            GROUP,
            TUI,
            STY
        ) = record
        return SimpleNamespace(
            GROUPCODE=GROUPCODE,
            GROUP=GROUP,
            TUI=TUI,
            STY=STY
        )

class MRCONSOParser(BaseUMLSParser):
    def parseLine(record):
        (
            CUI,
            LAT,
            TS,
            LUI,
            STT,
            SUI,
            ISPREF,
            AUI,
            SAUI,
            SCUI,
            SDUI,
            SAB,
            TTY,
            CODE,
            STR,
            SRL,
            SUPPRESS,
            CVF,
            _
        ) = record

        return SimpleNamespace(
            CUI=CUI,
            LAT=LAT,
            TS=TS,
            LUI=LUI,
            STT=STT,
            SUI=SUI,
            ISPREF=ISPREF,
            AUI=AUI,
            SAUI=SAUI,
            SCUI=SCUI,
            SDUI=SDUI,
            SAB=SAB,
            TTY=TTY,
            CODE=CODE,
            STR=STR,
            SRL=SRL,
            SUPPRESS=SUPPRESS,
            CVF=CVF
        )

class MRSTYParser(BaseUMLSParser):
    def parseLine(record):
        (
            CUI,
            TUI,
            STN,
            STY,
            ATUI,
            CVF,
            _
        ) = record
        return SimpleNamespace(
            CUI=CUI,
            TUI=TUI,
            STN=STN,
            STY=STY,
            ATUI=ATUI,
            CVF=CVF
        )
