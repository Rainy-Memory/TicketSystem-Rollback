//
// Created by Rainy Memory on 2021/4/13.
//

#ifndef TICKETSYSTEM_AUTOMATA_ADMINISTRATOR_H
#define TICKETSYSTEM_AUTOMATA_ADMINISTRATOR_H

#include "UserManager.h"
#include "TrainManager.h"
#include "OrderManager.h"

class Administrator {
    /*
     * class Administrator
     * --------------------------------------------------------
     * A class implements all functions, including two extra
     * commands: [clean], [exit].
     * This class used a Parser to parse commands, and three
     * managers to execute corresponding functions.
     * The name of parser, managers and Administrator in main.cpp
     * are credit to [Arknights], no copyright infringement intended.
     * This class passed OnlineJudge at 2021-05-30, 15:36:30.
     *
     */
private:
    Parser       *Ptilopsis = nullptr;
    UserManager  *Saria     = nullptr;
    TrainManager *Silence   = nullptr;
    OrderManager *Ifrit     = nullptr;
    rmstream defaultOut;
    const string path1 = "log_cmd1.dat";
    const string path2 = "log_cmd2.dat";
    string log_cmd_path = path1;
    std::fstream * fs = nullptr;
    
    using path_t = const std::string;
    
    path_t UserIndexPath     {"UserIndex.dat"};
    path_t UserStoragePath   {"UserStorage.dat"};
    path_t TrainIndexPath    {"TrainIndex.dat"};
    path_t TrainStoragePath  {"TrainStorage.dat"};
    path_t TrainTicketPath   {"TrainTicket.dat"};
    path_t TicketStoragePath {"TicketStorage.dat"};
    path_t TrainStationPath  {"TrainStation.dat"};
    path_t OrderIndexPath    {"OrderIndex.dat"};
    path_t OrderPendingPath  {"OrderPending.dat"};
    
    void initialize();
    
    void rollback(int timeStamp);
    
    void naiveRollback(int timeStamp);
    
    void clean();
    
    string getNewName() {
        return log_cmd_path == path1 ? path2 : path1;
    }

public:
    Administrator();
    
    ~Administrator();
    
    void runProgramme();
    
    void runNaiveRollback();
};

#endif //TICKETSYSTEM_AUTOMATA_ADMINISTRATOR_H
