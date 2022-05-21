//
// Created by Rainy Memory on 2021/4/13.
//

#include "Administrator.h"
#include <fstream>

void Administrator::initialize() {
    Ptilopsis = new Parser;
    Saria     = new UserManager(UserIndexPath, UserStoragePath, defaultOut);
    Silence   = new TrainManager(TrainIndexPath, TrainStoragePath, TrainTicketPath, TicketStoragePath, TrainStationPath, defaultOut);
    Ifrit     = new OrderManager(Saria, Silence, OrderIndexPath, OrderPendingPath, defaultOut);
}

void Administrator::clean() {
    Saria   -> clear();
    Silence -> clear();
    Ifrit   -> clear();
}

void Administrator::rollback(int timeStamp) {
    Saria   -> rollback(timeStamp);
    Silence -> rollback(timeStamp);
    Ifrit   -> rollback(timeStamp);
}

Administrator::Administrator() = default;

Administrator::~Administrator() {
    delete Ptilopsis;
    delete Saria;
    delete Silence;
    delete Ifrit;
    delete fs;
}

int RainyMemory::MemoryPoolTimeStamp::timeStamp = 0;

bool ioctl::disablePrint = false;

void Administrator::runProgramme() {
//    std::ios::sync_with_stdio(false);
//    freopen("../bin/data_new/basic_4/7.in", "r", stdin);
    initialize();
    const int CMD_SIZE = 5000;
    char cmd[CMD_SIZE];
    bool flag = true;
    int _t;
    while (flag) {
        memset(cmd, 0, sizeof(cmd));
        fgets(cmd, CMD_SIZE, stdin);
        Ptilopsis -> resetBuffer(cmd);
        RainyMemory::MemoryPoolTimeStamp::timeStamp = Ptilopsis -> getTimeStamp();
#ifdef debugBackDoor
        defaultOut << "[" << RainyMemory::MemoryPoolTimeStamp::timeStamp << "] ";
#endif
//        if (124577 == Ptilopsis -> getTimeStamp()) {
//            std::cerr << "a";
//        }
//        if (118956 == Ptilopsis -> getTimeStamp()) {
//            std::cerr << "a";
//        }
        switch (Ptilopsis -> getType()) {
            case Parser::ADDUSER:
                Saria   -> addUser(*Ptilopsis);
                break;
            case Parser::LOGIN:
                Saria   -> login(*Ptilopsis);
                break;
            case Parser::LOGOUT:
                Saria   -> logout(*Ptilopsis);
                break;
            case Parser::QUERYPROFILE:
                Saria   -> queryProfile(*Ptilopsis);
                break;
            case Parser::MODIFYPROFILE:
                Saria   -> modifyProfile(*Ptilopsis);
                break;
            case Parser::ADDTRAIN:
                Silence -> addTrain(*Ptilopsis);
                break;
            case Parser::RELEASETRAIN:
                Silence -> releaseTrain(*Ptilopsis);
                break;
            case Parser::QUERYTRAIN:
                Silence -> queryTrain(*Ptilopsis);
                break;
            case Parser::DELETETRAIN:
                Silence -> deleteTrain(*Ptilopsis);
                break;
            case Parser::QUERYTICKET:
                Silence -> queryTicket(*Ptilopsis);
                break;
            case Parser::QUERYTRANSFER:
                Silence -> queryTransfer(*Ptilopsis);
                break;
            case Parser::BUYTICKET:
                Ifrit   -> buyTicket(*Ptilopsis);
                break;
            case Parser::QUERYORDER:
                Ifrit   -> queryOrder(*Ptilopsis);
                break;
            case Parser::REFUNDTICKET:
                Ifrit   -> refundTicket(*Ptilopsis);
                break;
            case Parser::CLEAN:
                clean();
                defaultOut << "0" << endl;
                break;
            case Parser::EXIT:
                flag = false;
                defaultOut << "bye" << endl;
                break;
            case Parser::ROLLBACK:
                _t = (*Ptilopsis)("-t");
                if (_t <= Ptilopsis -> getTimeStamp()) {
                    rollback(_t);
                    defaultOut << "0" << endl;
                }
                else defaultOut << "-1" << endl;
                break;
            default:
                defaultOut << "[Error]Invalid command." << endl;
                break;
        }
    }
}

void Administrator::naiveRollback(int timeStamp) {
    bool execute = true;
    string debugStr;
    ioctl::disablePrint = true;
    fs -> open(log_cmd_path, std::ios::in);
    fs -> seekg(0);
    Parser rbp; // roll back parser
    auto * newFs = new std::fstream;
    newFs -> open(getNewName(), std::ios::out);
    while (true) {
        string cmd;
        getline(*fs, cmd);
        if (cmd.empty()) break;
        rbp.resetBuffer(cmd.c_str());
        if (rbp.getTimeStamp() >= timeStamp) execute = false;
        if (execute || rbp.getType() == Parser::EXIT || rbp.getType() == Parser::ROLLBACK) {
            (*newFs) << cmd << "\n";
            debugStr += (cmd + "\n");
        }
        if (execute) {
            switch (rbp.getType()) {
                case Parser::ADDUSER:
                    Saria   -> addUser(rbp);
                    break;
                case Parser::LOGIN:
                    Saria   -> login(rbp);
                    break;
                case Parser::LOGOUT:
                    Saria   -> logout(rbp);
                    break;
                case Parser::QUERYPROFILE:
                    Saria   -> queryProfile(rbp);
                    break;
                case Parser::MODIFYPROFILE:
                    Saria   -> modifyProfile(rbp);
                    break;
                case Parser::ADDTRAIN:
                    Silence -> addTrain(rbp);
                    break;
                case Parser::RELEASETRAIN:
                    Silence -> releaseTrain(rbp);
                    break;
                case Parser::QUERYTRAIN:
                    Silence -> queryTrain(rbp);
                    break;
                case Parser::DELETETRAIN:
                    Silence -> deleteTrain(rbp);
                    break;
                case Parser::QUERYTICKET:
                    Silence -> queryTicket(rbp);
                    break;
                case Parser::QUERYTRANSFER:
                    Silence -> queryTransfer(rbp);
                    break;
                case Parser::BUYTICKET:
                    Ifrit   -> buyTicket(rbp);
                    break;
                case Parser::QUERYORDER:
                    Ifrit   -> queryOrder(rbp);
                    break;
                case Parser::REFUNDTICKET:
                    Ifrit   -> refundTicket(rbp);
                    break;
                case Parser::EXIT:
                case Parser::ROLLBACK:
                    Saria   -> clearLoginPool();
                    break;
                default:
                    defaultOut << "[Error]Invalid command in rollback. cmd:" << endl << cmd << endl;
                    break;
            }
        }
    }
    newFs -> close();
    fs -> close();
    delete fs;
    fs = newFs;
    log_cmd_path = getNewName();
    ioctl::disablePrint = false;
}

void Administrator::runNaiveRollback() {
    fs = new std::fstream;
    fs -> open(log_cmd_path, std::ios::in);
    if (!(*fs)) {
        fs -> clear();
        fs -> close();
        fs -> open(log_cmd_path, std::ios::out);
    }
    fs -> close();
//    std::ios::sync_with_stdio(false);
    initialize();
    const int CMD_SIZE = 5000;
    char cmd[CMD_SIZE];
    bool flag = true;
    int _t;
    while (flag) {
        memset(cmd, 0, sizeof(cmd));
        fgets(cmd, CMD_SIZE, stdin);
        Ptilopsis -> resetBuffer(cmd);
        RainyMemory::MemoryPoolTimeStamp::timeStamp = Ptilopsis -> getTimeStamp();
#ifdef debugBackDoor
        defaultOut << "[" << RainyMemory::MemoryPoolTimeStamp::timeStamp << "] ";
#endif
//        if (8392 == Ptilopsis -> getTimeStamp()) {
//            std::cerr << "a";
//        }
        switch (Ptilopsis -> getType()) {
            case Parser::ADDUSER:
                Saria   -> addUser(*Ptilopsis);
                break;
            case Parser::LOGIN:
                Saria   -> login(*Ptilopsis);
                break;
            case Parser::LOGOUT:
                Saria   -> logout(*Ptilopsis);
                break;
            case Parser::QUERYPROFILE:
                Saria   -> queryProfile(*Ptilopsis);
                break;
            case Parser::MODIFYPROFILE:
                Saria   -> modifyProfile(*Ptilopsis);
                break;
            case Parser::ADDTRAIN:
                Silence -> addTrain(*Ptilopsis);
                break;
            case Parser::RELEASETRAIN:
                Silence -> releaseTrain(*Ptilopsis);
                break;
            case Parser::QUERYTRAIN:
                Silence -> queryTrain(*Ptilopsis);
                break;
            case Parser::DELETETRAIN:
                Silence -> deleteTrain(*Ptilopsis);
                break;
            case Parser::QUERYTICKET:
                Silence -> queryTicket(*Ptilopsis);
                break;
            case Parser::QUERYTRANSFER:
                Silence -> queryTransfer(*Ptilopsis);
                break;
            case Parser::BUYTICKET:
                Ifrit   -> buyTicket(*Ptilopsis);
                break;
            case Parser::QUERYORDER:
                Ifrit   -> queryOrder(*Ptilopsis);
                break;
            case Parser::REFUNDTICKET:
                Ifrit   -> refundTicket(*Ptilopsis);
                break;
            case Parser::CLEAN:
                clean();
                defaultOut << "0" << endl;
                break;
            case Parser::EXIT:
                flag = false;
                defaultOut << "bye" << endl;
                break;
            case Parser::ROLLBACK:
                _t = (*Ptilopsis)("-t");
                if (_t <= Ptilopsis -> getTimeStamp()) {
                    clean();
                    naiveRollback(_t);
                    Saria -> clearLoginPool();
                    defaultOut << "0" << endl;
                }
                else defaultOut << "-1" << endl;
                break;
            default:
                defaultOut << "[Error]Invalid command." << endl;
                break;
        }
        fs -> open(log_cmd_path, std::ios::in | std::ios::out | std::ios::app);
        (*fs) << cmd;
        fs -> close();
    }
}
