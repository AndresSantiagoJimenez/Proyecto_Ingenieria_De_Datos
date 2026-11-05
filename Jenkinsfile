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
                '''
            }
        }

        stage('Static Lint & Code Style') {
            steps {
                sh '''
                    . ${PYTHON_VENV}/bin/activate
                    ruff check src/ tests/
                '''
            }
        }

        stage('Unit Tests & Coverage') {
            steps {
                sh '''
                    . ${PYTHON_VENV}/bin/activate
                    pytest --cov=src --cov-report=xml:coverage.xml --cov-report=term
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

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            cleanWs deleteDirs: true, notFailBuild: true
        }
    }
}
