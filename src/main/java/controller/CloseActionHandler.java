package controller;

import dto.ActionDTO;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class CloseActionHandler implements ActionHandler{
    private final Logger LOGGER = LoggerFactory.getLogger(CloseActionHandler.class);
    private Store store;

    public CloseActionHandler(Store store) {
        this.store = store;
    }
    @Override
    public void handleAction(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "close action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
        oos.writeObject(resp);
        oos.flush();
        store.close();
    }
}