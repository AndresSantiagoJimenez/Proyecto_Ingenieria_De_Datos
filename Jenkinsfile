pipeline {
    agent any

    environment {
        PYTHON_VENV = "venv"
        SONAR_SCANNER_HOME = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Prepare Virtual Environment') {
            steps {
                sh '''
                    python3 -m venv ${PYTHON_VENV}
                    . ${PYTHON_VENV}/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements-dev.txt
                    pip install -e .
                '''
            }
        }

        stage('Static Lint & Code Style') {
            steps {
                sh '''
                    . ${PYTHON_VENV}/bin/activate
                    ruff check src/ tests/ scripts/ dags/
                '''
            }
        }

        stage('Unit Tests & Coverage') {
            steps {
                sh '''
                    . ${PYTHON_VENV}/bin/activate
                    pytest tests/ --cov=src --cov-report=xml:coverage.xml --cov-report=term --junitxml=test-results.xml
                '''
            }
        }

        stage('Validate Quality Gate Local') {
            steps {
                sh '''
                    . ${PYTHON_VENV}/bin/activate
                    python scripts/validate_quality_gate.py
                '''
            }
        }

        stage('SonarQube Analysis') {
            steps {
                withSonarQubeEnv('SonarQube') {
                    sh "${SONAR_SCANNER_HOME}/bin/sonar-scanner -Dproject.settings=sonar-project.properties"
                }
            }
        }

        stage('Quality Gate Approval') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            junit testResults: 'test-results.xml', allowEmptyResults: true
            archiveArtifacts artifacts: 'coverage.xml, test-results.xml', allowEmptyArchive: true
            cleanWs deleteDirs: true, notFailBuild: true
        }
    }
}
