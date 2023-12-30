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


void executeJob(const std::string& job, int k) {
    pid_t pid = fork();

    if (pid == -1) {
        throw std::runtime_error("Ошибка при создании процесса");
    }

    if (pid == 0) {
        std::cout << "ID job: " << getpid() << " " << job << "\n";
        std::cout << job << " Буду работать: " << k << " секунд." << "\n";
        sleep(k);
        exit(0);
    } else {
        std::cout << "Выполняется работа: " << job << std::endl;
        wait(NULL);
        std::cout << "Работа завершена: " << job << std::endl;
    } 
}


void processJob(const std::string& job, const std::unordered_map<std::string, std::vector<Json::String>>& dependencies, 
std::unordered_set<std::string>& visitedJobs) {

    std::cout << visitedJobs.size() << " " << dependencies.size() << std::endl;
    //int k = rand()%5;
    int k = 1;
    std::cout << k << "\n";

    if (visitedJobs.size() == dependencies.size()) {
        return;
    }
    
    if (visitedJobs.count(job) == 0) {
        visitedJobs.insert(job);
    }
    
    if (dependencies.count(job) == 1) {
        const std::vector<Json::String>& currentDependencies = dependencies.at(job); //Возвращает ссылку на элемент в заданном положении в векторе.

        std::cout << std::this_thread::get_id() << std::endl;
        std::cout << currentDependencies.size() << std::endl;


        if (currentDependencies.size() > 1 or currentDependencies.size() == 0) {
            std::vector<std::thread> threads;
            if(currentDependencies.size() == 0){
                std::cout << "popa" << "\n";

            }else{
                for (const auto& dependency : currentDependencies) {
                std::cout << "228\n";
                for(auto i : visitedJobs)
                    std::cout << i << std::endl;
                if (visitedJobs.count(dependency) == 0) {
                    visitedJobs.insert(dependency);
                    //std::cout << dependency << std::endl;
                    if (visitedJobs.size() == dependencies.size()) {
                        std::cout << "12345\n";
                        threads.emplace_back(std::thread(executeJob, dependency, k));//?
                        std::cout << std::this_thread::get_id() << " 12345" << std::endl;
                    } else {
                        std::cout << "55555\n";
                        threads.emplace_back(processJob, job, std::ref(dependencies), std::ref(visitedJobs));
                        std::cout << std::this_thread::get_id() << " 55555" <<  std::endl;
                    }
                }
            }
            for (auto& thread : threads) {
                std::cout << "wait///" <<  std::endl;
                thread.join();
            }
        }
            
        } else {
            for (const auto& dependency : currentDependencies) {
                std::cout << "337\n";
                if (visitedJobs.count(dependency) == 0) {
                    visitedJobs.insert(dependency);
                    if (visitedJobs.size() == dependencies.size()) {
                        executeJob(dependency, k);
                    } else {
                        processJob(dependency, dependencies, visitedJobs);
                    }
                }
            }
        }
    }
    std::cout << "end.\n";
    executeJob(job, k);
}

bool hasCycle(const Json::Value& jobs, const std::string& currentJob, std::set<std::string>& visited, std::set<std::string>& recursionStack) {
    visited.insert(currentJob);
    recursionStack.insert(currentJob);

    const Json::Value& dependencies = jobs[currentJob]["dependencies"]; //?
    for (const auto& dependency : dependencies) {
        const std::string& dependencyJob = dependency.asString();

        if (recursionStack.count(dependencyJob) == 1) {
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
    int k;
    std::ifstream file("f_test.json");
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

    std::cout << "ID потока: " << getpid() << "\n";

    std::vector<std::string> jobs;
    for (const auto& job : data["jobs"].getMemberNames()) {
        //std::cout << job << "\n";
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
        //std::cout << "!!!!" << job << "\n";
        if(dependencies_map[job].size() == 0 && visitedJobs.count(job) == 0){
            std::vector<std::thread> threads;
            for (const std::string& job : jobs) {
                if(dependencies_map[job].size() == 0){
                    visitedJobs.insert(job);
                    k = rand()%5;
                    threads.emplace_back(executeJob, job, k);
                }
            }
            for (auto& thread : threads) {
                thread.join();
            }
            //std::cout << job << ": " << dependencies_map[job].size() << "\n";
        }
        else{
            if(dependencies_map[job].size() != 0 && visitedJobs.count(job) == 0){
                //std::cout << job << "<- job" << "\n";
                std::vector<std::thread> threads;
                
                std::vector<std::string> using_jobs;
                for(const std::string& JOB : jobs){
                    //std::cout << "????" << JOB << "\n";
                    const std::vector<Json::String>& currentDependencies = dependencies_map.at(JOB);
                    for(const auto& elem : currentDependencies){
                        //std::cout << JOB << "<---- job " << elem << "\n";
                        if(visitedJobs.count(elem) == 1 && visitedJobs.count(JOB) == 0){
                            //std::cout << JOB << "<- job " << elem << "\n";
                            using_jobs.push_back(JOB);
                            break;
                        }
                    }
                }

                for(const auto& elem_job : using_jobs){
                    visitedJobs.insert(elem_job);
                    k = rand()%5;
                    threads.emplace_back(executeJob, elem_job, k);
                }
                for (auto& thread : threads) {
                    thread.join();
                }
            }
        }
    }

    std::cout << "Все работы завершены" << std::endl;

    return 0;
}
 