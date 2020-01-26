#!/usr/bin/env bash

PLANT_UML_DOCKER=miy4/plantuml:latest

PUML=plantuml

command -v ${PUML} >/dev/null 2>&1 || {
  echo >&2 "plantuml not installed, fall-back to image $(docker pull -q ${PLANT_UML_DOCKER})";
  echo
  PUML="docker run -v ${PWD}:/work -w /work --rm ${PLANT_UML_DOCKER}";
}

$PUML -r "**/*.puml"
