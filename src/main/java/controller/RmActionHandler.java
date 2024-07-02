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

public class RmActionHandler implements ActionHandler{
    private final Logger LOGGER = LoggerFactory.getLogger(RmActionHandler.class);
    private Store store;

    public RmActionHandler(Store store) {
        this.store = store;
    }
    @Override
    public void handleAction(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        store.rm(dto.getKey());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "rm action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
        oos.writeObject(resp);
        oos.flush();
    }
}
