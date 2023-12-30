#include <iostream>
#include <fstream>
#include <json/json.h>
#include <set>
#include <functional>
#include <vector>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include "unistd.h"
#include "sys/wait.h"
#include "stdio.h"
#include "stdlib.h"
#include "ctype.h"


void executeJob(const std::string& job) {
    pid_t pid = fork();

    if (pid == -1) {
        throw std::runtime_error("Ошибка при создании процесса");
    }

    if (pid == 0) {
        sleep(1);
        exit(0);
    } else {
        std::cout << "Выполняется работа: " << job << std::endl;
        sleep(1);
        std::cout << "Работа завершена: " << job << std::endl;
        wait(NULL);
    }
}


void processJob(const std::string& job, const std::unordered_map<std::string, std::vector<Json::String>>& dependencies, 
std::unordered_set<std::string>& visitedJobs) {

    if (visitedJobs.size() == dependencies.size()) {
        return;
    }
    
    if (visitedJobs.count(job) == 0) {
        visitedJobs.insert(job);
    }
    
    if (dependencies.count(job) > 0) {
        const std::vector<Json::String>& currentDependencies = dependencies.at(job);

        if (currentDependencies.size() > 1) {
            std::vector<std::thread> threads;
            for (const auto& dependency : currentDependencies) {
                if (visitedJobs.count(dependency) == 0) {
                    visitedJobs.insert(dependency);
                    if (visitedJobs.size() == dependencies.size()) {
                        threads.emplace_back(executeJob, dependency);
                    } else {
                        threads.emplace_back(processJob, job, std::ref(dependencies), std::ref(visitedJobs));
                    }
                }   
            }
            for (auto& thread : threads) {
                thread.join();
            }
        } else {
            for (const auto& dependency : currentDependencies) {
                if (visitedJobs.count(dependency) == 0) {
                    visitedJobs.insert(dependency);
                    if (visitedJobs.size() == dependencies.size()) {
                        executeJob(dependency);
                    } else {
                        processJob(dependency, dependencies, visitedJobs);
                    }
                }
            }
        }
    }

    executeJob(job);
}

bool hasCycle(const Json::Value& jobs, const std::string& currentJob, std::set<std::string>& visited, std::set<std::string>& recursionStack) {
    visited.insert(currentJob);
    recursionStack.insert(currentJob);

    const Json::Value& dependencies = jobs[currentJob]["dependencies"];
    for (const auto& dependency : dependencies) {
        const std::string& dependencyJob = dependency.asString();

        if (recursionStack.count(dependencyJob)) {
            return true;
        }

        if (!visited.count(dependencyJob) && hasCycle(jobs, dependencyJob, visited, recursionStack)) {
            return true;
        }
    }

    recursionStack.erase(currentJob);
    return false;
}

bool isValidDAG(const Json::Value& jobs) {
    std::set<std::string> visited;
    std::set<std::string> recursionStack;

    for (const auto& job : jobs.getMemberNames()) {
        if (!visited.count(job) && hasCycle(jobs, job, visited, recursionStack)) {
            return false;
        }
    }

    return true;
}

bool hasOnlyOneComponent(const Json::Value& jobs) {
    std::unordered_map<std::string, std::vector<std::string>> adjacencyList;
    std::set<std::string> visited;

    // Строим список смежности
    for (const auto& job : jobs.getMemberNames()) {
        const Json::Value& dependencies = jobs[job]["dependencies"];
        for (const auto& dependency : dependencies) {
            adjacencyList[dependency.asString()].push_back(job);
            adjacencyList[job].push_back(dependency.asString());
        }
    }

    int componentCount = 0;

    // Обходим граф и считаем количество компонент связности
    for (const auto& job : jobs.getMemberNames()) {
        if (visited.count(job) == 0) {
            std::set<std::string> component;
            std::queue<std::string> q;

            q.push(job);
            visited.insert(job);

            while (!q.empty()) {
                std::string currentJob = q.front();
                q.pop();
                component.insert(currentJob);

                for (const auto& neighbor : adjacencyList[currentJob]) {
                    if (visited.count(neighbor) == 0) {
                        q.push(neighbor);
                        visited.insert(neighbor);
                    }
                }
            }

            componentCount++;
        }
    }

    return componentCount > 1;
}

bool hasStartAndEndJobs(const Json::Value& jobs) {
    std::set<std::string> startJobs;
    std::set<std::string> endJobs;

    for (const auto& job : jobs.getMemberNames()) {
        const Json::Value& dependencies = jobs[job]["dependencies"];
        if (dependencies.empty()) {
            startJobs.insert(job);
        }

        for (const auto& dependency : dependencies) {
            endJobs.erase(dependency.asString());
        }

        endJobs.insert(job);
    }

    return !startJobs.empty() && !endJobs.empty();
}

int main() {
    std::ifstream file("diman.json");
    Json::Value data;
    file >> data;
    Json::Value jobb = data["jobs"];


    if (!isValidDAG(jobb)) {
        throw std::runtime_error("DAG содержит цикл");
    }

    if (hasOnlyOneComponent(jobb)) {
        throw std::runtime_error("DAG имеет больше чем один компонент связности");
    }

    if (!hasStartAndEndJobs(jobb)) {
        throw std::runtime_error("DAG не имеет начального и конечного джоба");
    }

    std::cout << "DAG валидный, можем продолжать" << std::endl << std::endl;
    sleep(1);

    int n = data["jobs"].getMemberNames().size();
    std::vector<std::string> jobs;
    for (const auto& job : data["jobs"].getMemberNames()) {
        jobs.push_back(job);
    }



    std::unordered_map<std::string, std::vector<Json::String>> dependencies_map;
    std::unordered_set<std::string> visitedJobs;

    for (const auto& job : data["jobs"].getMemberNames()) {
        std::vector<Json::String> dependencies;
        for (const auto& dependency : data["jobs"][job]["dependencies"]) {
            dependencies.push_back(dependency.asString());
        }
        dependencies_map[job] = dependencies;
    }


    for (const std::string& job : jobs) {
        processJob(job, dependencies_map, visitedJobs);
    }

    std::cout << "Все работы завершены" << std::endl;

    return 0;
}
