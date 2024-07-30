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

public class SetActionHandler implements ActionHandler{
    private final Logger LOGGER = LoggerFactory.getLogger(SetActionHandler.class);
    private Store store;

    public SetActionHandler(Store store) {
        this.store = store;
    }
    @Override
    public void handleAction(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        store.set(dto.getKey(), dto.getValue());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "set action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, dto.getValue());
        oos.writeObject(resp);
        oos.flush();
    }
}
